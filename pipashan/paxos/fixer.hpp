#ifndef PIPASHAN_PAXOS_FIXER_INCLUDED
#define PIPASHAN_PAXOS_FIXER_INCLUDED

#include <optional>
#include <map>
#include <string>
#include "../identifier.hpp"

namespace pipashan::paxos_details
{
	/// Conflict fixer
	class fixer
	{
		struct proposal
		{
			std::optional<identifier> in;
			std::map<std::string, std::optional<identifier>> ext;

			void accept(bool internal, const std::string& grouping_key, const identifier& key)
			{
				if (internal)
				{
					if (in == key)
						in.reset();
				}
				else
				{
					auto i = ext.find(grouping_key);
					if (i != ext.cend())
					{
						if (i->second == key)
							i->second.reset();
					}
				}
			}
		};
	public:
		using nodecaps_interface = pipashan::abs::nodecaps_interface;
		using nodeconn = net::nodeconn;
		using buffer = proto::buffer;

		fixer(nodecaps_interface* nodecaps) :
			nodecaps_(nodecaps)
		{
		}

		void resize(const std::vector<nodeconn::pointer>& nodes)
		{
			for (auto i = nodes_.cbegin(); i != nodes_.cend();)
			{
				if (_m_find(nodes, i->first))
					i = nodes_.erase(i);
				else
					++i;
			}

			for (auto& conn : nodes)
				nodes_[conn->id()];
		}

		void remove(const std::string& node)
		{
			std::vector<std::string> grouping_keys;

			auto i = nodes_.find(node);
			if (i == nodes_.cend())
				return;

			//Get the grouping_keys of the node which is about to be deleted.
			for (auto& keys : i->second.ext)
				grouping_keys.push_back(keys.first);

			nodes_.erase(node);
			_m_detect(false, {});

			for (auto& key : grouping_keys)
				_m_detect(true, key);
		}

		std::optional<identifier> update(const std::string& node, bool internal, const std::string& grouping_key, const std::optional<identifier>& key)
		{
			if (key && accepting_.count(key.value()))
				return {};

			auto i = nodes_.find(node);
			if(i == nodes_.cend())
				return {};

			if (internal)
			{
				if (i->second.in)
					_m_clear(true, grouping_key, nodes_[node].in.value());
				i->second.in = key;
			}
			else
			{
				if(grouping_key.empty())
					return {};

				auto u = i->second.ext.find(grouping_key);
				if(u == i->second.ext.cend())
					return {};

				if (u->second)
					_m_clear(true, grouping_key, u->second.value());

				u->second = key;
			}

			return _m_detect(internal, grouping_key);
		}

		std::optional<identifier> update(bool internal, const std::string& grouping_key, const std::optional<identifier>& key)
		{
			if (key && accepting_.count(key.value()))
				return {};

			if (internal)
			{
				if (self_.in)
					_m_clear(true, grouping_key, self_.in.value());

				self_.in = key;
			}
			else
			{
				auto i = self_.ext.find(grouping_key);
				if (i != self_.ext.cend() && i->second)
					_m_clear(false, grouping_key, i->second.value());

				self_.ext[grouping_key] = key;
			}
			return _m_detect(internal, grouping_key);
		}

		void begin_accept(bool internal, const std::string& grouping_key, const identifier& key)
		{
			accepting_.insert(key);

#ifdef SHOWLOG_FIXER
			auto log = nodecaps_->log("fixer");
			log.msg("begin accept: internal=", internal, ", grouping_key=", grouping_key, ", key=", key.short_hex());
#endif
			for (auto& prop : nodes_)
			{
				prop.second.accept(internal, grouping_key, key);
			}

			self_.accept(internal, grouping_key, key);
		}

		void end_accept(bool internal, const std::string& grouping_key, const identifier& key)
		{
			accepting_.insert(key);

#ifdef SHOWLOG_FIXER
			auto log = nodecaps_->log("fixer");
			log.msg("end accept: internal=", internal, ", grouping_key=", grouping_key, ", key=", key.short_hex());
#endif
			for (auto& prop : nodes_)
			{
				prop.second.accept(internal, grouping_key, key);
			}

			self_.accept(internal, grouping_key, key);

			accepting_.erase(key);
		}
	private:
		static bool _m_find(const std::vector<nodeconn::pointer>& nodes, const std::string& id)
		{
			auto i = std::find_if(nodes.cbegin(), nodes.cend(), [id](auto& conn) {
				return id == conn->id();
				});

			return i != nodes.cend();
		}

		std::size_t _m_clear(bool internal, const std::string& grouping_key, const identifier& key)
		{
			std::size_t n = 0;
			if (internal)
			{
				if (self_.in == key)
				{
					++n;
					self_.in.reset();
				}

				for (auto& node : nodes_)
				{
					if (node.second.in == key)
					{
						++n;
						node.second.in.reset();
					}
				}
			}
			else
			{
				auto i = self_.ext.find(grouping_key);
				if (i != self_.ext.cend() && i->second == key)
				{
					++n;
					i->second.reset();
				}

				for (auto& node : nodes_)
				{
					auto u = node.second.ext.find(grouping_key);
					if (u != node.second.ext.cend() && u->second == key)
					{
						++n;
						u->second.reset();
					}
				}
			}
			return n;
		}

		std::optional<identifier> _m_detect(bool internal, const std::string& grouping_key)
		{
			//Unnecessary to detect the conflicts if there isn't an acceptor to be connected
			if (nodes_.empty())
				return {};

			auto const threshold = (nodes_.size() + 1) / 2 + 1;
			std::map<identifier, std::size_t> count;

			std::stringstream ss;

			ss << (internal ? "internal" : "external") << " conflict{\n    table {\n";

			if (internal)
			{
				if (!self_.in)
					return {};

				ss << "       node(" << nodecaps_->identity() << ") : " << self_.in->short_hex() << "\n";

				count[self_.in.value()] = 1;

				for (auto& prop : nodes_)
				{
					if (!prop.second.in)
						return {};

					ss << "       node(" << prop.first << ") : " << prop.second.in->short_hex() << "\n";

					auto size = ++(count[prop.second.in.value()]);
					if (size >= threshold)
						return {};
				}

				ss << "    }\n";
			}
			else
			{
				auto i = self_.ext.find(grouping_key);
				if ((i == self_.ext.cend()) || (!i->second))
					return {};

				auto& value = i->second.value();

				ss << "       node(" << nodecaps_->identity() << ") : " << value.short_hex() << "\n";

				count[value] = 1;

				for (auto& prop : nodes_)
				{
					auto u = prop.second.ext.find(grouping_key);
					if (u == prop.second.ext.cend() || !u->second)
						return {};

					ss << "       node(" << prop.first << ") : " << u->second->short_hex() << "\n";

					auto size = ++(count[u->second.value()]);
					if (size >= threshold)
						return {};
				}

				ss << "    }\n";
			}

			//conflicted
			auto log = nodecaps_->log("fixer");

			auto i = count.begin();

			auto winner = i->first;

			for (++i; i != count.end(); ++i)
				if (i->first < winner)
					winner = i->first;

			for (auto& cnt : count)
				ss << "    " << cnt.first.short_hex() << " : " << cnt.second << "\n";
			ss << "} accepting = ";

			if (0 != accepting_.count(winner))
				ss << "yes";
			else
				ss << "no";
			
			log.msg(ss.str());

			return winner;
		}

	private:
		nodecaps_interface* const nodecaps_;
		std::map<std::string, proposal> nodes_;

		std::set<identifier> accepting_;

		proposal self_;
	};


}

#endif