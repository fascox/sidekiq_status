require 'spec_helper'

describe SidekiqStatus::Worker do
  def run_sidekiq(show_sidekiq_output = ENV['SHOW_SIDEKIQ'])
    log_to = show_sidekiq_output ? STDOUT : GEM_ROOT.join('log/spawned_sidekiq.log').to_s
    command = 'bundle exec sidekiq -r ./boot.rb --concurrency 1'

    Process.spawn(
      command,
      :chdir => DUMMY_APP_ROOT,
      :err => :out,
      :out => log_to,
      :pgroup => true
    )
  end

  def with_sidekiq_running
    pid = run_sidekiq

    begin
      yield(pid)
    ensure
      Process.kill('TERM', -Process.getpgid(pid))
      Process.wait(pid)
    end
  end

  context "integrates seamlessly with sidekiq and" do
    it "allows to query for complete job status and request payload" do
      some_value = 'some_value'
      jid = TestWorker1.perform_async(some_value)
      container = SidekiqStatus::Container.load(jid)
      expect(container).to be_waiting

      with_sidekiq_running do
        wait{ container.reload.complete? }

        expect(container.total).to eq(200)
        expect(container.payload).to eq(some_value)
      end
    end

    it "allows to query for working job status and request payload" do
      redis_key = 'SomeRedisKey'

      jid = TestWorker2.perform_async(redis_key)
      container = SidekiqStatus::Container.load(jid)
      expect(container).to be_waiting

      with_sidekiq_running do
        wait{ container.reload.working? }

        Sidekiq.redis{ |conn| conn.set(redis_key, 10) }
        wait{  container.reload.at == 10 }
        expect(container.message).to eq('Some message at 10')

        Sidekiq.redis{ |conn| conn.set(redis_key, 50) }
        wait{ container.reload.at == 50 }
        expect(container.message).to eq('Some message at 50')

        Sidekiq.redis{ |conn| conn.set(redis_key, 'stop') }
        wait{ container.reload.complete? }
        expect(container).to be_complete
        expect(container.message).to be_nil
      end
    end
  end
end
