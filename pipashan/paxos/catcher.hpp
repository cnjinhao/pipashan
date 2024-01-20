#ifndef PIPASHAN_PAXOS_CATCHER_INCLUDED
#define PIPASHAN_PAXOS_CATCHER_INCLUDED


#include "task_queue.hpp"

namespace pipashan::paxos_details
{
	class catcher
	{
		enum class stages
		{
			begin,
			transferring_snapshot,
			transferring_journal,
		};
	public:
		catcher(task_queue& taskque):
			taskque_(taskque)
		{
		}

		bool paused() const noexcept
		{
			return paused_;
		}

		void set_pause() noexcept
		{
			paused_ = true;
		}

		/// Fetchs catchup data
		/**
		 * @param paxcth The parameters of catchup
		 * @param completed Indicates snapshot and journal data are all returned
		 * @return catchup data
		 */
		std::optional<catchup_data> catchup_data(const proto::payload::paxos_catchup& paxcth, bool& completed)
		{
			completed = false;
			if(stages::begin == stage_)
			{
				//Check
				if(taskque_.determine(paxcth.cursors))
					stage_ = stages::transferring_snapshot;
				else
					stage_ = stages::transferring_journal;
			}
			
			if(stages::transferring_snapshot == stage_)
			{
				bool is_snapshot_completed;
				auto cdata = taskque_.fetch_snapshot(paxcth.internal_off, paxcth.external_off, paxcth.anchors_off, is_snapshot_completed);

				if(!cdata)
					stage_ = is_snapshot_completed ? stages::transferring_journal : stages::transferring_snapshot;

				return cdata;
			}
			else if(stages::transferring_journal != stage_)
				return {};

			//now, it's in stage transferring_journal
			
			bool has_left;
			auto cdata = taskque_.fetch_catchup_data(paxcth.cursors, true, has_left);
			completed = !has_left;
			return cdata;
		}
	private:
		task_queue& taskque_;
		stages stage_{ stages::begin };
		bool paused_{ false };
	};
}

#endif