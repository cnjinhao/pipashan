#ifndef PIPASHAN_PAXOS_taskque_INCLUDED
#define PIPASHAN_PAXOS_taskque_INCLUDED

#include "fstream.hpp"
#include "../log.hpp"
#include <deque>
#include <mutex>
#include <string>
#include <optional>
#include "../nodecaps.hpp"
#include "catchup_data.hpp"

namespace pipashan::paxos_details
{
	class task_queue
	{
		using file_size_t = proto::file_size_t;

		///The metadata which is at the beginning of journal file.
		struct metadata
		{
			static constexpr std::uint32_t version_value = 230825;
			std::uint32_t version;	//230825
			std::uint32_t anchor_size;
			std::uint32_t anchors_bytes;
			std::uint32_t queue_position;
		};

		struct task_metadata
		{
			bool			rollbacked;
			bool			internal;
			std::uint64_t	time;
			std::uint16_t	time_seq;
			std::uint8_t	node_id_len;
			std::uint8_t	key_len;
			std::uint16_t	data_bytes;
		};

		/// a task structure in memory
		struct task_lite
		{
			file_size_t pos;
			std::size_t length;
			std::uint64_t time;
			std::uint16_t time_seq;
			std::string node;
			identifier key;
		};

		enum class state_types
		{
			normal,
			assigning_snapshot,
			snapshot_done,
		};
	public:
		using data_metrics = catchup_data::data_metrics;

		class invoker_interface
		{
		public:
			virtual ~invoker_interface() = default;

			virtual bool task(const identifier& key, bool internal, std::string_view data) = 0;
			virtual void internal(const std::filesystem::path&) = 0;
			virtual void external(const std::filesystem::path&) = 0;
		};

		struct snapshot_offs
		{
			std::uint64_t internal;
			std::uint64_t external;
			std::uint64_t anchors;
		};

		task_queue(const std::string& name, invoker_interface& inv, pipashan::abs::nodecaps_interface* nodecaps):
			paxos_(name),
			nodecaps_(nodecaps),
			invoker_(inv)
		{
			std::filesystem::path path{nodecaps->working_path() / "paxos" / name};

			std::error_code err;
			std::filesystem::create_directories(path, err);

			if(!fslog_.open(path / "queue"))
			{
				auto msg = "Failed to open task queue file of consensus(" + paxos_ + "):" + fslog_.error_message();

				auto log = nodecaps->log("acceptor");
				log.err(msg);

				fslog_.close();
				throw std::logic_error(msg);
			}

			metadata md;
			md.version = 0;

			if(fslog_.bytes() >= sizeof(metadata))
			{
				fslog_.seek(0);
				fslog_.read(&md, sizeof(md));				
			}

			if(md.version != md.version_value)
			{
				fslog_.truncate(0);

				md.version = md.version_value;
				md.anchor_size = 0;
				md.anchors_bytes = 0;
				md.queue_position = sizeof(md);

				fslog_.write(&md, sizeof(md));
			}

			ssworks_.anchors_bytes = md.anchors_bytes;

			ssworks_.internal_bytes = file_size(_m_internal_path(), err);
			if(err)
				ssworks_.internal_bytes = 0;

			ssworks_.external_bytes = file_size(_m_external_path(), err);
			if(err)
				ssworks_.external_bytes = 0;
		}

		bool apply()
		{
			fslog_.seek(0);

			metadata md;
			fslog_.read(&md, sizeof(md));

			if(md.version != md.version_value)
				return false;

			std::lock_guard lock{ mutex_ };

			_m_fast_scan(md.queue_position);

			_m_read_anchors();

			for(auto & an : anchors_)
			{
				if(cursors_.count(an.first) == 0)
					cursors_[an.first] = an.second;
			}

			invoker_.internal(_m_internal_path());
			invoker_.external(_m_external_path());

			std::unique_ptr<char[]> buf;
			std::size_t bufsize = 0;

			for(auto & task : tasks_)
			{
				fslog_.seek(task.pos);

				task_metadata tm;

				fslog_.read(reinterpret_cast<char*>(&tm), sizeof(tm));

				//Don't verify the CRC, so it's unnecessary to read last 2 bytes.
				if(tm.data_bytes > bufsize)
				{
					buf.reset(new char[tm.data_bytes]);
					bufsize = tm.data_bytes;
				}

				fslog_.seek(tm.key_len + tm.node_id_len, SEEK_CUR);

				fslog_.read(buf.get(), tm.data_bytes);

				//Skip the CRC
				fslog_.seek(2, SEEK_CUR);

				if(!invoker_.task(task.key, tm.internal, std::string_view{buf.get(), tm.data_bytes}))
					return false;
			}

			return true;
		}

		/// Marks a task as rollbacked
		void rollback(const identifier& key)
		{
			std::size_t idx = 1;
			for(auto i = tasks_.crbegin(); i != tasks_.crend(); ++i)
			{
				if(i->key == key)
				{
					fslog_.seek(i->pos);
					bool rollbacked = true;
					fslog_.write(&rollbacked, sizeof(bool));

					tasks_.erase(tasks_.cbegin() + (tasks_.size() - idx));
					return;
				}

				++idx;
			}
		}

		bool empty() const noexcept
		{
			std::lock_guard lock{ mutex_ };

			return tasks_.empty() && anchors_.empty();
		}

		std::size_t size() const
		{
			std::lock_guard lock{ mutex_ };

			return tasks_.size();
		}

		void reset_write_state()
		{
			std::lock_guard lock{ mutex_ };
			state_ = state_types::normal;
		}

		bool write_rawdata(const std::string_view& bv)
		{
			auto log = nodecaps_->log("taskqueue");
			if(bv.empty())
				return true;

			catchup_data_view cdata{bv.data(), bv.size()};

			//Result of verified data
			if(cdata.empty())
			{
				log.err("write_rawdata, buffer size = ", bv.size(), " is not satisfied.");
				return false;
			}

			auto metrics = cdata.metrics();

			auto const current_snapshot_bytes = metrics->internal_bytes + metrics->external_bytes + metrics->anchors_bytes;

			std::lock_guard lock{ mutex_ };

			if(current_snapshot_bytes > 0)
			{
				if(state_types::normal == state_)
				{
					state_ = state_types::assigning_snapshot;

					std::error_code err;
					std::filesystem::remove(_m_internal_path(), err);
					std::filesystem::remove(_m_external_path(), err);

					fslog_.truncate(0);

					metadata md;
					md.version = md.version_value;
					md.anchor_size = 0;
					md.anchors_bytes = 0;
					md.queue_position = sizeof(md);

					fslog_.write(&md, sizeof(md));

					//The snapshot and queue are going to be updated, clear
					//the current states.
					anchors_.clear();
					cursors_.clear();
					tasks_.clear();

					ssworks_.internal_bytes = 0;
					ssworks_.external_bytes = 0;
					ssworks_.anchors_bytes = 0;
				}
				else if(state_types::snapshot_done == state_)
				{
					log.msg("<warning> still received catchup snapshot data");
					return true;
				}

				if(metrics->internal_bytes > 0)
				{
					std::ofstream os{_m_internal_path(), std::ios::binary | std::ios::app};
					os.write(cdata.internal(), cdata.internal_bytes());
					ssworks_.internal_bytes += metrics->internal_bytes;
				}

				if(metrics->external_bytes > 0)
				{
					std::ofstream os{_m_external_path(), std::ios::binary | std::ios::app};
					os.write(cdata.external(), cdata.external_bytes());
					ssworks_.external_bytes += metrics->external_bytes;
				}

				if(metrics->anchors_bytes > 0)
				{
					//Only writes the anchor data, it doesn't update the metadata.
					//the metadata will be updated when the snapshot is transferred completely.
					fslog_.seekend();
					fslog_.write(cdata.anchors(), cdata.anchors_bytes());
					ssworks_.anchors_bytes += metrics->anchors_bytes;

					fslog_.fsync();
				}
			}
			

			if((state_types::assigning_snapshot == state_) && ((0 == current_snapshot_bytes) || cdata.journal_bytes()))
			{
				//Snapshot is transferred completely.
				//Perform deserialize for applying the states.

				//Build the anchors and update the metadata
				auto anchors_bytes = fslog_.bytes() - sizeof(metadata);

				if(!_m_read_anchors(anchors_bytes))
				{
					//Todo, error 
					throw std::logic_error("anchors buffer is corrupted");
				}

				cursors_ = anchors_;

				//Update metadata
				fslog_.seek(0);
				metadata md;
				fslog_.read(&md, sizeof(md));

				md.anchor_size = static_cast<std::uint32_t>(anchors_.size());
				md.anchors_bytes = static_cast<std::uint32_t>(anchors_bytes);

				fslog_.seek(0);
				fslog_.write(&md, sizeof(md));
				fslog_.fsync();

				invoker_.internal(_m_internal_path());
				invoker_.external(_m_external_path());

				state_ = state_types::snapshot_done;
			}			

			if(0 == metrics->journal_bytes)
				return true;

			if(!_m_check_journal(metrics, bv.size()))
			{
				log.err("the rawdata is corrupted");
				return false;
			}

			//The pointer to the first journal task
			auto buf = bv.data() + sizeof(data_metrics) + metrics->internal_bytes + metrics->external_bytes;

			auto pos = fslog_.seekend();
			fslog_.write(buf, metrics->journal_bytes);
			fslog_.fsync();

			auto len = metrics->journal_bytes;

			while(len)
			{
				//No need to check the len, because the buffer has been checked.

				auto tm = reinterpret_cast<const task_metadata*>(buf);
				if(tm->rollbacked)
				{
					auto tasklen = sizeof(task_metadata) + tm->data_bytes + tm->key_len + tm->node_id_len + 2;
					len -= tasklen;
					buf += tasklen;
					continue;
				}

				//generate the paxos_meta for the cursor
				////node,key,data
				proto::payload::paxos_meta meta;

				if(identifier::size() != tm->key_len)
					throw std::logic_error("invalid key length");

				meta.key.assign(buf + sizeof(task_metadata) + tm->node_id_len);
				meta.internal = tm->internal;
				meta.paxos = paxos_;
				meta.time = tm->time;
				meta.time_seq = tm->time_seq;

				std::string sender{buf + sizeof(task_metadata), tm->node_id_len};

				cursors_[sender] = meta;

				//Add this request to the task list.

				task_lite task;
				task.key = meta.key;
				task.length = sizeof(task_metadata) + tm->data_bytes + tm->key_len + tm->node_id_len + 2;
				task.node = sender;
				task.pos = pos;
				task.time = tm->time;
				task.time_seq = tm->time_seq;

				tasks_.push_back(task);

				pos += task.length;
			
				std::string_view body{buf + sizeof(task_metadata) + tm->node_id_len + tm->key_len, tm->data_bytes};

				invoker_.task(task.key, tm->internal, body);

				len -= task.length;
				buf += task.length;				
			}
			return true;
		}

		std::deque<task_lite> journal_tasks(const catchup_data_view& cdata, bool check_buffer)
		{
			//Assume the buffer has already been checked if check_buffer is false.
			if(check_buffer && !_m_check_journal(cdata.metrics(), cdata.bytes()))
			{
				return {};
			}

			auto len = cdata.metrics()->journal_bytes;

			//The pointer to the first journal task
			auto pj = cdata.journal();

			std::deque<task_lite> tasks;
			
			while(len)
			{
				auto tm = reinterpret_cast<const task_metadata*>(pj);

				if(tm->rollbacked)
				{
					auto tasklen = sizeof(task_metadata) + tm->data_bytes + tm->key_len + tm->node_id_len + 2;
					len -= tasklen;
					pj += tasklen;
					continue;
				}

				//generate the paxos_meta for the cursor
				////node,key,data
				proto::payload::paxos_meta meta;

				if(identifier::size() != tm->key_len)
					throw std::logic_error("invalid key length");

				std::string sender{pj + sizeof(task_metadata), tm->node_id_len};

				//Add this request to the task list.

				task_lite task;
				task.key = meta.key;
				task.length = sizeof(task_metadata) + tm->data_bytes + tm->key_len + tm->node_id_len + 2;
				task.node = sender;
				task.time = tm->time;
				task.time_seq = tm->time_seq;

				tasks.push_back(task);

				len -= task.length;
				pj += task.length;	
			}

			return tasks;
		}

		void write(const std::string& node_id, const proto::payload::paxos_meta& meta, const std::string& data)
		{
			std::lock_guard lock{ mutex_ };

			fslog_.seekend();
			_m_push(node_id, meta, data);

			cursors_[node_id] = meta;

		}


		/// Determines whether the snapshot is transferred.
		bool determine(const std::vector<proto::payload::paxos_cursor>& cursors) const
		{
			std::lock_guard lock{ mutex_ };

			if(anchors_.empty())
				return false;

			if(anchors_.size() > cursors.size())
				return true;

			for(auto & an : anchors_)
			{
				auto i = std::find_if(cursors.cbegin(), cursors.cend(), [&an](auto & cur){
					return (an.first == cur.node);
				});

				if(i == cursors.cend() || (an.second.time > i->time))
					return true;

				if(an.second.time == i->time && an.second.time_seq > i->time_seq)
					return true;				
			}

			return false;
		}

		/// Determines the number of data in catchup queue from the specified cursors
		/**
		 *  
		 */
		std::optional<std::size_t> catchup_data_size(const std::vector<proto::payload::paxos_cursor>& cursors) const noexcept
		{
			std::lock_guard lock{ mutex_ };

			if(cursors.empty() || (anchors_.size() > cursors.size()))
			{
				return tasks_.size();
			}

			std::size_t matched_anchors = 0;

			for(auto & cur: cursors)
			{
				auto i = anchors_.find(cur.node);
				if(i == anchors_.cend())
					continue;

				if(i->second.time > cur.time || (i->second.time == cur.time && i->second.time_seq > cur.time))
					return {};

				++matched_anchors;
			}

			if(matched_anchors < anchors_.size())
				return {};

			std::size_t idx;
			auto scattered = _m_fetch_scattered(cursors, idx);

			return scattered.size() + tasks_.size() - idx;
		}

		std::optional<snapshot_offs> snapshot_offsets() const
		{
			std::lock_guard lock{ mutex_ };

			if(state_types::assigning_snapshot == state_)
			{
				snapshot_offs soff;
				soff.internal = ssworks_.internal_bytes;
				soff.external = ssworks_.external_bytes;
				soff.anchors = ssworks_.anchors_bytes;
				return soff;
			}
			return {};
		}

		std::optional<catchup_data> fetch_snapshot(std::uint64_t internal_off, std::uint64_t external_off, std::uint64_t anchors_off, bool& completed)
		{
			//No journal
			catchup_data cdata{ 0 };

			std::uint64_t internal_bytes = ssworks_.internal_bytes;
			std::uint64_t external_bytes = ssworks_.external_bytes;
			std::uint64_t anchors_bytes = ssworks_.anchors_bytes;

			//Calculates the rest bytes to transfer
			if(internal_off < internal_bytes)
				internal_bytes -= internal_off;
			else
				internal_bytes = 0;

			if(external_off < external_bytes)
				external_bytes -= external_off;
			else
				external_bytes = 0;

			if(anchors_off < anchors_bytes)
				anchors_bytes -= anchors_off;
			else
				anchors_bytes = 0;

			auto const total_rest_bytes = internal_bytes + external_bytes + anchors_bytes;
			
			completed = false;
			if(0 == total_rest_bytes)
			{
				completed = true;

				//Returns an empty catchup_data
				return cdata;
			}

			std::uint64_t capacity = proto::max_packet_bytes - 100;

			//Calculates the bytes to transfer
			if(internal_bytes >= capacity)
			{
				internal_bytes = capacity;
				external_bytes = 0;
				anchors_bytes = 0;
			}
			else
			{
				capacity -= internal_bytes;

				if(external_bytes >= capacity)
				{
					external_bytes = capacity;
					anchors_bytes = 0;
				}
				else
				{
					capacity -= external_bytes;

					if(anchors_bytes > capacity)
						anchors_bytes = capacity;
				}
			}

			completed = (total_rest_bytes == internal_bytes + external_bytes + anchors_bytes);


			//Only allocates memory for snapshot
			cdata.alloc(internal_bytes, external_bytes, anchors_bytes, 0);	

			if(internal_bytes)
			{
				fstream fs{_m_internal_path()};

				if(fs.bytes() < internal_off + internal_bytes)
					//Error the internal file is corrupted.
					return {};

				fs.seek(internal_off);
				fs.read(cdata.internal(), internal_bytes);
			}

			if(external_bytes)
			{
				fstream fs{_m_external_path()};

				if(fs.bytes() < external_off + external_bytes)
					//Error the internal file is corrupted.
					return {};

				fs.seek(external_off);
				fs.read(cdata.external(), external_bytes);
			}

			if(anchors_bytes)
			{
				metadata md;
				fslog_.seek(0);
				fslog_.read(&md, sizeof(md));

				if((md.anchors_bytes != ssworks_.anchors_bytes) || (ssworks_.anchors_bytes < anchors_off + anchors_bytes))
					//Error the internal file is corrupted.
					return {};
				
				fslog_.seek(anchors_off + sizeof(md));
				fslog_.read(cdata.anchors(), anchors_bytes);

				//Check anchors

			}

			return cdata;			
		}

		std::optional<catchup_data> fetch_catchup_data(const std::vector<proto::payload::paxos_cursor>& cursors, bool ignore_snapshot, bool& has_left)
		{
			std::lock_guard lock{ mutex_ };

			auto log = nodecaps_->log(paxos_, "taskqueue");

			has_left = false;

			if(!ignore_snapshot)
			{
				bool completed;
				if((!anchors_.empty()) || (anchors_.size() > cursors.size()))
					return fetch_snapshot(0, 0, 0, completed);

				std::size_t matched_anchors = 0;

				for(auto & cur: cursors)
				{
					auto i = anchors_.find(cur.node);
					if(i == anchors_.cend())
						continue;

					if(i->second.time > cur.time || (i->second.time == cur.time && i->second.time_seq > cur.time))
					{
						log.msg("catch me up, anchor(", i->first, ") is newer than cursors");
						return fetch_snapshot(0, 0, 0, completed);
					}

					++matched_anchors;
				}

				if(matched_anchors < anchors_.size())
				{
					log.msg("catch me up, anchors is newer than cursors");
					//transfer snapshot
					return fetch_snapshot(0, 0, 0, completed);
				}
			}

			if(tasks_.empty())
			{
				return {};
			}

			//scattered tasks
			std::size_t last = 0;

			if(!cursors.empty())
			{
				auto scattered = _m_fetch_scattered_data(cursors, last);

				has_left = (last < tasks_.size());

				if(scattered)
					return scattered;

				if(last == tasks_.size())
				{
					// No catchup data
					catchup_data cdata{0};
					cdata.alloc(0, 0, 0, 0);
					return cdata;
				}
				else if(last > tasks_.size())
				{
					if(paxos_ == "demo")
						log.err("failed to fetch scattered data");
					return {};
				}
			}

			std::size_t bytes = 0;
			std::size_t begin = last, end = tasks_.size();
			for(std::size_t i = begin; i < end; ++i)
			{
				if(tasks_[i].length + bytes > proto::max_packet_bytes - 100)
				{
					end = i;
					break;
				}
				bytes += tasks_[i].length;
			}

			catchup_data cdata{ end - begin};
			cdata.alloc(0, 0, 0, bytes);

			fslog_.seek(tasks_[begin].pos);
			fslog_.read(cdata.journal(), cdata.journal_bytes());
			has_left = (end < tasks_.size());

			if(!_m_check_journal(cdata.metrics(), cdata.bytes()))
			{
				log.err("failed to verify catchup data");
				return {};
			}

			//msg<<"fetch "<<transfer_size<<" of "<<tasks_.size()<<std::endl;
			return cdata;
		}

		std::map<std::string, proto::payload::paxos_meta> cursors() const
		{
			std::lock_guard lock{ mutex_ };
			return cursors_;
		}

		template<typename SnapshotGenerator>
		void update_snapshot(SnapshotGenerator sgen)
		{
			auto internal_path = _m_internal_path();
			auto external_path = _m_external_path();
			std::lock_guard lock{ mutex_ };

			sgen(internal_path, external_path);

			std::error_code err;
			ssworks_.internal_bytes = static_cast<std::size_t>(std::filesystem::file_size(internal_path, err));
			ssworks_.external_bytes = static_cast<std::size_t>(std::filesystem::file_size(external_path, err));

			anchors_ = cursors_;
			
			// Clear the in-memory task queue and cursors.
			cursors_.clear();
			tasks_.clear();

			//Clear the file 
			fslog_.truncate(0);

			//Rebuild the journal file
			metadata md;

			md.version = md.version_value;
			md.anchor_size = 0;
			md.anchors_bytes = 0;
			md.queue_position = sizeof(md);

			fslog_.write(&md, sizeof(md));

			if(!anchors_.empty())
			{
				md.anchor_size = static_cast<std::uint32_t>(anchors_.size());

				_m_write_anchors();
				md.anchors_bytes = static_cast<std::uint32_t>(fslog_.bytes() - sizeof(md));
				md.queue_position = static_cast<std::uint32_t>(fslog_.bytes());
			}

			ssworks_.anchors_bytes = md.anchors_bytes;

			//Write back the metadata.
			fslog_.seek(0);
			fslog_.write(&md, sizeof(md));
		}
	private:
		std::filesystem::path _m_internal_path() const
		{
			return nodecaps_->working_path() / "paxos" / paxos_ / "internal";
		}

		std::filesystem::path _m_external_path() const
		{
			return nodecaps_->working_path() / "paxos" / paxos_ / "external";
		}

		std::vector<std::deque<task_lite>::const_iterator> _m_fetch_scattered(const std::vector<proto::payload::paxos_cursor>& cursors, std::size_t& idx) const
		{
			idx = tasks_.size() + 1;

			std::vector<std::deque<task_lite>::const_iterator> scattered;

			auto begin = tasks_.cend();
			for(auto i = tasks_.cbegin(); i != tasks_.cend(); ++i)
			{
				for(auto & cur: cursors)
				{
					if((cur.time != i->time) || (cur.time_seq != i->time_seq))
						continue;

					if(cur.key == i->key)
					{
						begin = i;
						break;
					}
				}

				if(begin != tasks_.cend())
					break;
			}

			if(begin == tasks_.cend())
				return {};

			if(cursors.size() == 1)
			{
				idx = begin - tasks_.cbegin() + 1;
				return {};
			}

			std::set<std::string> reached_cursors;
			reached_cursors.insert(begin->node);

			for(auto i = begin + 1; i != tasks_.cend(); ++i)
			{
				if(reached_cursors.size() == cursors.size())
				{
					idx = std::distance(tasks_.cbegin(), i);
					return scattered;
				}

				if(reached_cursors.count(i->node))
				{
					scattered.push_back(i);
					continue;
				}

				for(auto & cur: cursors)
				{
					if(reached_cursors.count(cur.node))
						continue;

					if(cur.time == i->time && cur.time_seq == i->time_seq && cur.key == i->key)
					{
						reached_cursors.insert(i->node);
						break;
					}
				}
			}

			if(reached_cursors.size() == cursors.size())
				idx = tasks_.size();
			else
				idx = tasks_.size() + 1;

			return scattered;
		}

		std::optional<catchup_data> _m_fetch_scattered_data(const std::vector<proto::payload::paxos_cursor>& cursors, std::size_t& idx)
		{
			auto scattered = _m_fetch_scattered(cursors, idx);

			if(scattered.empty())
				return {};

			std::size_t bytes = 0;
			for(auto & sc : scattered)
				bytes += sc->length;

			auto log = nodecaps_->log("taskqueue");
			log.msg("fetch scattered bytes:", bytes);

			catchup_data cdata{ scattered.size()};

			cdata.alloc(0, 0, 0, bytes);

			auto p = cdata.journal();

			for(auto & sc : scattered)
			{
				fslog_.seek(sc->pos);
				fslog_.read(p, sc->length);

				p += sc->length;
			}

			_m_check_journal(cdata.metrics(), cdata.bytes());
			return cdata;
		}

		/// Checks the journal buffer
		bool _m_check_journal(const data_metrics* dm, file_size_t len)
		{
			if(len < sizeof(data_metrics))
				return false;

			if(dm->internal_bytes + dm->external_bytes + dm->journal_bytes + sizeof(data_metrics) > len)
				return false;

			auto p = reinterpret_cast<const char*>(dm) + sizeof(data_metrics) + dm->internal_bytes + dm->external_bytes;
			len = dm->journal_bytes;

			while(len)
			{
				if(len < sizeof(task_metadata))
					return false;

				auto tm = reinterpret_cast<const task_metadata*>(p);

				auto x = sizeof(*tm) + tm->data_bytes + tm->key_len + tm->node_id_len + 2;

				if(len < x)
					return false;

				p += x;
				len -= x;
			}
			return true;
		}

		void _m_write_anchors()
		{
			task_metadata tm;

			for(auto & an : anchors_)
			{
				tm.time = an.second.time;
				tm.time_seq = an.second.time_seq;
				tm.internal = an.second.internal;
				tm.node_id_len = static_cast<std::uint8_t>(an.first.size());
				tm.key_len = static_cast<std::uint8_t>(an.second.key.size());
				tm.data_bytes = 0;

				fslog_.write(&tm, sizeof(tm));

				fslog_.write(an.first.data(), an.first.size());
				fslog_.write(an.second.key.data(), an.second.key.size());
			}
		}

		void _m_read_anchors()
		{
			metadata md;
			fslog_.seek(0);
			fslog_.read(&md, sizeof(md));

			std::string node;
			task_metadata tm;

			anchors_.clear();

			for(std::size_t i = 0; i < md.anchor_size; ++i)
			{
				fslog_.read(&tm, sizeof(tm));

				node.resize(tm.node_id_len);
				fslog_.read(node.data(), tm.node_id_len);

				auto & an = anchors_[node];
				an.time = tm.time;
				an.time_seq = tm.time_seq;
				an.internal = tm.internal;

				if(identifier::size() != tm.key_len)
					throw std::logic_error("invalid key size");

				fslog_.read(reinterpret_cast<char*>(an.key.data()), tm.key_len);
			}
		}

		bool _m_read_anchors(file_size_t anchors_bytes)
		{
			fslog_.seek(sizeof(metadata));

			std::string node;
			task_metadata tm;

			anchors_.clear();

			while(anchors_bytes)
			{
				if(anchors_bytes <= sizeof(tm))
				{
					anchors_.clear();
					return false;
				}

				fslog_.read(&tm, sizeof(tm));

				if(anchors_bytes < sizeof(tm) + tm.node_id_len + tm.key_len)
				{
					anchors_.clear();
					return false;
				}
				else if(tm.node_id_len == 0 || tm.key_len == 0)
					return false;

				node.resize(tm.node_id_len);

				fslog_.read(node.data(), tm.node_id_len);

				auto & an = anchors_[node];
				an.paxos = paxos_;
				an.time = tm.time;
				an.time_seq = tm.time_seq;
				an.internal = tm.internal;

				if(tm.key_len != identifier::size())
					throw std::logic_error("invalid key size");

				fslog_.read(an.key.data(), tm.key_len);

				anchors_bytes -= sizeof(tm) + tm.node_id_len + tm.key_len;
			}

			return true;
		}

		void _m_push(const std::string& node_id, const proto::payload::paxos_meta& meta, const std::string& data)
		{
			task_lite task;
			task.pos = fslog_.tellp();

			task_metadata tm;

			tm.rollbacked = false;
			tm.time = meta.time;
			tm.time_seq = meta.time_seq;
			tm.internal = meta.internal;
			tm.node_id_len = static_cast<std::uint8_t>(node_id.size());
			tm.key_len = static_cast<std::uint8_t>(meta.key.size());
			tm.data_bytes = static_cast<std::uint16_t>(data.size());

			fslog_.write(&tm, sizeof(tm));

			//node,key,data

			fslog_.write(node_id.data(), node_id.size());
			fslog_.write(meta.key.data(), meta.key.size());
			fslog_.write(data.data(), data.size());

			//Add a crc	
			std::uint16_t len = sizeof(tm) + tm.node_id_len + tm.key_len + tm.data_bytes;
			fslog_.write(&len, 2);

			task.time = meta.time;
			task.time_seq = meta.time_seq;
			task.node = node_id;
			task.key = meta.key;

			task.length = sizeof(task_metadata) + meta.key.size() + node_id.size() + data.size() + 2; 

			tasks_.push_back(task);
		}

		std::size_t _m_read(task_lite& task, bool& rollbacked)
		{
			task.pos = fslog_.tellp();

			task_metadata tm;
			fslog_.read(&tm, sizeof(tm));

			task.length = sizeof(task_metadata) + tm.data_bytes + tm.key_len + tm.node_id_len + 2;

			if(tm.rollbacked)
			{
				rollbacked = true;
				return task.length;
			}

			rollbacked = false;

			task.time = tm.time;
			task.time_seq = tm.time_seq;

			//node, key, data

			task.node.resize(tm.node_id_len);
			fslog_.read(task.node.data(), tm.node_id_len);

			if(tm.key_len != identifier::size())
				throw std::logic_error("invalid key size");

			fslog_.read(task.key.data(), identifier::size());

			// Moves to the CRC position
			fslog_.seek(tm.data_bytes, SEEK_CUR);

			//std::uint32_t len = sizeof(tm) + tm.data_bytes + tm.key_len + tm.node_id_len;

			//Read and check the CRC
			std::uint16_t crc = 0;
			fslog_.read(&crc, 2);

			if(task.length != crc + 2)
				return 0;

			auto & meta = cursors_[task.node];
			meta.internal = tm.internal;
			meta.key = task.key;
			meta.paxos = paxos_;
			meta.time = tm.time;
			meta.time_seq = tm.time_seq;
			
			//Returns the size of task and CRC
			return task.length;
		}

		/// Scans the log file to create the queue
		bool _m_fast_scan(std::uint64_t queue_pos)
		{
			if(fslog_.bytes() < queue_pos)
				return false;

			auto const bytes = fslog_.bytes() - queue_pos;

			fslog_.seek(queue_pos);

			std::size_t read_bytes = 0;

			std::string node_id;
			
			task_lite task;
			while(read_bytes < bytes)
			{
				bool rollbacked;
				auto len = _m_read(task, rollbacked);

				if(0 == len)
				{
					fslog_.truncate(task.pos);
					break;
				}

				read_bytes += len;

				if(!rollbacked)
					tasks_.push_back(task);
			}

			return true;
		}

	private:
		const std::string paxos_;

		pipashan::abs::nodecaps_interface* const nodecaps_;
		invoker_interface& invoker_;

		mutable std::recursive_mutex mutex_;

		state_types state_{ state_types::normal };

		//Snapshot
		struct snapshotworks
		{
			std::uint64_t internal_bytes{ 0 };
			std::uint64_t external_bytes{ 0 };
			std::uint64_t anchors_bytes{ 0 };
		}ssworks_;

		fstream fslog_;
		std::map<std::string, proto::payload::paxos_meta> anchors_;
		std::map<std::string, proto::payload::paxos_meta> cursors_;
		std::deque<task_lite> tasks_;


	};
}

#endif