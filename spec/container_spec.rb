# -*- encoding : utf-8 -*-
require 'spec_helper'

def test_container(container, hash, jid = nil)
  hash.reject { |k, v| k == :last_updated_at }.find do |k, v|
    expect(container.send(k)).to eq(v)
  end

  expect(container.last_updated_at).to eq(Time.at(hash['last_updated_at'])) if hash['last_updated_at']
  expect(container.jid).to eq(jid) if jid
end


describe SidekiqStatus::Container do
  let(:jid) { "c2db8b1b460608fb32d76b7a" }
  let(:status_key) { described_class.status_key(jid) }
  let(:sample_json_hash) do
    {
        'args'            => ['arg1', 'arg2'],
        'worker'          => 'SidekiqStatus::Worker',
        'queue'           => '',

        'status'          => "completed",
        'at'              => 50,
        'total'           => 200,
        'message'         => "Some message",

        'payload'         => {},
        'last_updated_at' => 1344855831
    }
  end

  specify ".status_key" do
    jid = SecureRandom.base64
    expect(described_class.status_key(jid)).to eq("sidekiq_status:#{jid}")
  end

  specify ".kill_key" do
    expect(described_class.kill_key).to eq(described_class::KILL_KEY)
  end


  context "finders" do
    let!(:containers) do
      described_class::STATUS_NAMES.inject({}) do |accum, status_name|
        container = described_class.create()
        container.update_attributes(:status => status_name)

        accum[status_name] = container
        accum
      end
    end

    specify ".size" do
      expect(described_class.size).to eq(containers.size)
    end

    specify ".status_jids" do
      expected = containers.values.map(&:jid).map{ |jid| [jid, anything()] }
      expect(described_class.status_jids).to match_array(expected)
      expect(described_class.status_jids(0, 0).size).to eq(1)
    end

    specify ".statuses" do
      expect(described_class.statuses).to be_all{|st| st.is_a?(described_class) }
      expect(described_class.statuses.size).to eq(containers.size)
      expect(described_class.statuses(0, 0).size).to eq(1)
    end

    describe ".delete" do
      before do
        expect(described_class.status_jids.map(&:first)).to match_array(containers.values.map(&:jid))
      end

      specify "deletes jobs in specific status" do
        statuses_to_delete = ['waiting', 'complete']
        described_class.delete(statuses_to_delete)

        expect(described_class.status_jids.map(&:first)).to match_array(containers.
            reject{ |status_name, container|  statuses_to_delete.include?(status_name) }.
            values.
            map(&:jid))
      end

      specify "deletes jobs in all statuses" do
        described_class.delete()

        expect(described_class.status_jids).to be_empty
      end
    end
  end

  specify ".create" do
    expect(SecureRandom).to receive(:hex).with(12).and_return(jid)
    args = ['arg1', 'arg2', {arg3: 'val3'}]

    container = described_class.create('args' => args)
    expect(container).to be_a(described_class)
    expect(container.args).to eq(args)

    # Check default values are set
    test_container(container, described_class::DEFAULTS.reject{|k, v| k == 'args' }, jid)

    Sidekiq.redis do |conn|
      expect(conn.exists(status_key)).to be_truthy
    end
  end

  describe ".load" do
    it "raises StatusNotFound exception if status is missing in Redis" do
      expect { described_class.load(jid) }.to raise_exception(described_class::StatusNotFound, jid)
    end

    it "loads a container from the redis key" do
      json = MultiJson.dump(sample_json_hash)
      Sidekiq.redis { |conn| conn.set(status_key, json) }

      container = described_class.load(jid)
      test_container(container, sample_json_hash, jid)
    end

    it "cleans up unprocessed expired kill requests as well" do
      Sidekiq.redis do |conn|
        conn.zadd(described_class.kill_key, [
            [(Time.now - described_class.ttl - 1).to_i, 'a'],
            [(Time.now - described_class.ttl + 1).to_i, 'b'],
        ]
        )
      end

      json = MultiJson.dump(sample_json_hash)
      Sidekiq.redis { |conn| conn.set(status_key, json) }
      described_class.load(jid)

      Sidekiq.redis do |conn|
        expect(conn.zscore(described_class.kill_key, 'a')).to be_nil
        expect(conn.zscore(described_class.kill_key, 'b')).not_to be_nil
      end
    end
  end

  specify "#dump" do
    hash = sample_json_hash.reject{ |k, v| k == 'last_updated_at' }
    container = described_class.new(jid, hash)
    dump = container.send(:dump)
    expect(dump).to eq(hash.merge('last_updated_at' => Time.now.to_i))
  end

  specify "#save saves container to Redis" do
    hash = sample_json_hash.reject{ |k, v| k == 'last_updated_at' }
    described_class.new(jid, hash).save

    result = Sidekiq.redis{ |conn| conn.get(status_key) }
    result = MultiJson.load(result)

    expect(result).to eq(hash.merge('last_updated_at' => Time.now.to_i))

    Sidekiq.redis{ |conn| expect(conn.ttl(status_key)).to be >= 0 }
  end

  specify "#delete" do
    Sidekiq.redis do |conn|
      conn.set(status_key, "something")
      conn.zadd(described_class.kill_key, 0, jid)
    end

    container = described_class.new(jid)
    container.delete

    Sidekiq.redis do |conn|
      expect(conn.exists(status_key)).to be_falsey
      expect(conn.zscore(described_class.kill_key, jid)).to be_nil
    end
  end

  specify "#request_kill, #should_kill?, #killable?" do
    container = described_class.new(jid)
    expect(container.kill_requested?).to be_falsey
    expect(container).to be_killable

    Sidekiq.redis do |conn|
      expect(conn.zscore(described_class.kill_key, jid)).to be_nil
    end


    container.request_kill

    Sidekiq.redis do |conn|
      expect(conn.zscore(described_class.kill_key, jid)).to eq(Time.now.to_i)
    end
    expect(container).to be_kill_requested
    expect(container).not_to be_killable
  end

  specify "#kill" do
    container = described_class.new(jid)
    container.request_kill
    Sidekiq.redis do |conn|
      expect(conn.zscore(described_class.kill_key, jid)).to eq(Time.now.to_i)
    end
    expect(container.status).not_to eq('killed')


    container.kill

    Sidekiq.redis do |conn|
      expect(conn.zscore(described_class.kill_key, jid)).to be_nil
    end

    expect(container.status).to eq('killed')
    expect(described_class.load(jid).status).to eq('killed')
  end

  specify "#pct_complete" do
    container = described_class.new(jid)
    container.at = 1
    container.total = 100
    expect(container.pct_complete).to eq(1)

    container.at = 5
    container.total = 200
    expect(container.pct_complete).to eq(3) # 2.5.round(0) => 3
  end

  context "setters" do
    let(:container) { described_class.new(jid) }

    describe "#at=" do
      it "sets numeric value" do
        container.total = 100
        container.at = 3
        expect(container.at).to eq(3)
        expect(container.total).to eq(100)
      end

      it "raises ArgumentError otherwise" do
        expect{ container.at = "Wrong" }.to raise_exception(ArgumentError)
      end

      it "adjusts total if its less than new at" do
        container.total = 200
        container.at = 250
        expect(container.total).to eq(250)
      end
    end

    describe "#total=" do
      it "sets numeric value" do
        container.total = 50
        expect(container.total).to eq(50)
      end

      it "raises ArgumentError otherwise" do
        expect{ container.total = "Wrong" }.to raise_exception(ArgumentError)
      end
    end

    describe "#status=" do
      described_class::STATUS_NAMES.each do |status|
        it "sets status #{status.inspect}" do
          container.status = status
          expect(container.status).to eq(status)
        end
      end

      it "raises ArgumentError otherwise" do
        expect{ container.status = 'Wrong' }.to raise_exception(ArgumentError)
      end
    end

    specify "#message=" do
      container.message = 'abcd'
      expect(container.message).to eq('abcd')

      container.message = nil
      expect(container.message).to be_nil

      message = double('Message', :to_s => 'to_s')
      container.message = message
      expect(container.message).to eq('to_s')
    end

    specify "#payload=" do
      expect(container).to respond_to(:payload=)
    end

    specify "update_attributes" do
      container.update_attributes(:at => 1, 'total' => 3, :message => 'msg', 'status' => 'working')
      reloaded_container = described_class.load(container.jid)

      expect(reloaded_container.at).to eq(1)
      expect(reloaded_container.total).to eq(3)
      expect(reloaded_container.message).to eq('msg')
      expect(reloaded_container.status).to eq('working')

      expect{ container.update_attributes(:at => 'Invalid') }.to raise_exception(ArgumentError)
    end
  end

  context "predicates" do
    described_class::STATUS_NAMES.each do |status_name1|
      context "status is #{status_name1}" do
        subject{ described_class.create().tap{|c| c.status = status_name1} }

        its("#{status_name1}?") { should be_truthy }

        (described_class::STATUS_NAMES - [status_name1]).each do |status_name2|
          its("#{status_name2}?") { should be_falsey }
        end
      end
    end
  end
end