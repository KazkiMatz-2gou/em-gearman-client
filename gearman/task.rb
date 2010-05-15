module EventMachine
  module Protocols
    module Gearman

      # = Task
      #
      # == Description
      # A task submitted to a Gearman job server.
      class Task

        ##
        # Create a new Task object.
        #
        # @param func  function name
        # @param arg   argument to the function
        # @param opts  hash of additional options
        def initialize(conn, func, arg='')
          @conn = conn
          @func = func.to_s
          @arg = arg or ''  # TODO: use something more ref-like?
          %w{on_complete on_fail on_retry on_exception on_status on_warning on_data
              uniq retry_count priority background}.map {|s| s.to_sym }.each do |k|
            instance_variable_set "@#{k}", nil
          end
          @timeout = 60
          @priority = nil
          @background = false
          @retry_count ||= 0
          @successful = false
          @retries_done = 0
          @hash = nil
        end
        attr_accessor :uniq, :retry_count, :timeout, :priority, :background
        attr_reader :successful, :func, :arg, :retries_done

        ##
        # Internal function to add a task.
        #
        # @param task         Task to add
        # @param reset_state  should we reset task state?  true if we're adding a
        #                     new task; false if we're rescheduling one that's
        #                     failed
        # @return             true if the task was created successfully, false
        #                     otherwise
        def queue!
          @conn.add_task self
          @conn.send_req
        end

        ##
        # Internal method to reset this task's state so it can be run again.
        # Called by TaskSet#add_task.
        def reset_state
          @retries_done = 0
          @successful = false
          self
        end

        ##
        # Set a block of code to be executed when this task completes
        # successfully.  The returned data will be passed to the block.
        def on_complete(&f)
          @on_complete = f
        end

        ##
        # Set a block of code to be executed when this task fails.
        def on_fail(&f)
          @on_fail = f
        end

        ##
        # Set a block of code to be executed when this task is retried after
        # failing.  The number of retries that have been attempted (including the
        # current one) will be passed to the block.
        def on_retry(&f)
          @on_retry = f
        end

        ##
        # Set a block of code to be executed when a remote exception is sent by a worker.
        # The block will receive the message of the exception passed from the worker.
        # The user can return true for retrying or false to mark it as finished
        #
        # NOTE: this is actually deprecated, cf. https://bugs.launchpad.net/gearmand/+bug/405732
        #
        def on_exception(&f)
          @on_exception = f
        end

        ##
        # Set a block of code to be executed when we receive a status update for
        # this task.  The block will receive two arguments, a numerator and
        # denominator describing the task's status.
        def on_status(&f)
          @on_status = f
        end

        ##
        # Set a block of code to be executed when we receive a warning from a worker.
        # It is recommended for workers to send work_warning, followed by work_fail if
        # an exception occurs on their side. Don't expect this behavior from workers NOT
        # using this very library ATM, though. (cf. https://bugs.launchpad.net/gearmand/+bug/405732)
        def on_warning(&f)
          @on_warning = f
        end

        ##
        # Set a block of code to be executed when we receive a (partial) data packet for this task.
        # The data received will be passed as an argument to the block.
        def on_data(&f)
          @on_data = f
        end

        ##
        # Handle completion of the task.
        #
        # @param data  data returned from the server (doesn't include handle)
        def handle_completion(data)
          @successful = true
          @on_complete.call(data) if @on_complete
          self
        end

        ##
        # Record a failure and check whether we should be retried.
        #
        # @return  true if we should be resubmitted; false otherwise
        def handle_failure
          if @retries_done >= @retry_count
            @on_fail.call if @on_fail
            return false
          end
          @retries_done += 1
          @on_retry.call(@retries_done) if @on_retry
          true
        end

        ##
        # Record an exception.
        #
        def handle_exception(exception)
          @on_exception.call(exception) if @on_exception
          self
        end

        ##
        # Handle a status update for the task.
        def handle_status(numerator, denominator)
          @on_status.call(numerator, denominator) if @on_status
          self
        end

        ##
        # Handle a warning.
        #
        def handle_warning(message)
          @on_warning.call(message) if @on_warning
          self
        end

        ##
        # Handle (partial) data
        def handle_data(data)
          @on_data.call(data) if @on_data
          self
        end

        ##
        # Return a hash that we can use to execute identical tasks on the same
        # job server.
        #
        # @return  hashed value, based on @arg if @uniq is '-', on @uniq if it's
        #          set to something else, and just nil if @uniq is nil
        def get_uniq_hash
          return @hash if @hash
          merge_on = (@uniq and @uniq == '-') ? @arg : @uniq
          @hash = merge_on ? merge_on.hash.to_s : ''
        end

        ##
        # Construct a packet to submit this task to a job server.
        #
        # @return            String representation of packet
        def get_submit_packet()
          mode = 'submit_job'
          if(@priority)
            if(@priority == :high)
              mode += "_high"
            elsif(@priority == :low)
              mode += "_low"
            end
          end

          if(@background)
            mode += "_bg"
          end

          Util::pack_request(mode, [func, get_uniq_hash, arg].join("\0"))
        end
      end

    end
  end
end

