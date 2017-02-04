require "helper"
require "delayed/backend/active_record"

describe Delayed::Backend::ActiveRecord::Job do
  it_behaves_like "a delayed_job backend"

  describe "configuration" do
    describe "reserve_sql_strategy" do
      it "allows :optimized_sql" do
        Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy = :optimized_sql
        expect(Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy).to eq(:optimized_sql)
      end

      it "allows :default_sql" do
        Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy = :default_sql
        expect(Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy).to eq(:default_sql)
      end

      it "raises an argument error on invalid entry" do
        expect { Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy = :invald }.to raise_error(ArgumentError)
      end
    end
  end

  describe "reserve_with_scope" do
    let(:worker) { double(name: "worker01", read_ahead: 1) }
    let(:scope)  { double(limit: limit, where: double(update_all: nil)) }
    let(:limit)  { double(job: job, update_all: nil) }
    let(:job)    { double(id: 1) }

    before do
      allow(Delayed::Backend::ActiveRecord::Job.connection).to receive(:adapter_name).at_least(:once).and_return(dbms)
      Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy = reserve_sql_strategy
    end

    context "with reserve_sql_strategy option set to :optimized_sql (default)" do
      let(:reserve_sql_strategy) { :optimized_sql }

      context "for mysql adapters" do
        let(:dbms) { "MySQL" }

        it "uses the optimized sql version" do
          expect(Delayed::Backend::ActiveRecord::Job).to_not receive(:reserve_with_scope_using_default_sql)
          Delayed::Backend::ActiveRecord::Job.reserve_with_scope(scope, worker, Time.now)
        end
      end

      context "for a dbms without a specific implementation" do
        let(:dbms) { "OtherDB" }

        it "uses the plain sql version" do
          expect(Delayed::Backend::ActiveRecord::Job).to receive(:reserve_with_scope_using_default_sql).once
          Delayed::Backend::ActiveRecord::Job.reserve_with_scope(scope, worker, Time.now)
        end
      end
    end

    context "with reserve_sql_strategy option set to :default_sql" do
      let(:dbms) { "MySQL" }
      let(:reserve_sql_strategy) { :default_sql }

      it "uses the plain sql version" do
        expect(Delayed::Backend::ActiveRecord::Job).to receive(:reserve_with_scope_using_default_sql).once
        Delayed::Backend::ActiveRecord::Job.reserve_with_scope(scope, worker, Time.now)
      end
    end
  end

  context "db_time_now" do
    after do
      Time.zone = nil
      ActiveRecord::Base.default_timezone = :local
    end

    it "returns time in current time zone if set" do
      Time.zone = "Eastern Time (US & Canada)"
      expect(%(EST EDT)).to include(Delayed::Job.db_time_now.zone)
    end

    it "returns UTC time if that is the AR default" do
      Time.zone = nil
      ActiveRecord::Base.default_timezone = :utc
      expect(Delayed::Backend::ActiveRecord::Job.db_time_now.zone).to eq "UTC"
    end

    it "returns local time if that is the AR default" do
      Time.zone = "Central Time (US & Canada)"
      ActiveRecord::Base.default_timezone = :local
      expect(%w(CST CDT)).to include(Delayed::Backend::ActiveRecord::Job.db_time_now.zone)
    end
  end

  describe "after_fork" do
    it "calls reconnect on the connection" do
      expect(ActiveRecord::Base).to receive(:establish_connection)
      Delayed::Backend::ActiveRecord::Job.after_fork
    end
  end

  describe "enqueue" do
    it "allows enqueue hook to modify job at DB level" do
      later = described_class.db_time_now + 20.minutes
      job = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: EnqueueJobMod.new
      expect(Delayed::Backend::ActiveRecord::Job.find(job.id).run_at).to be_within(1).of(later)
    end
  end

  if ::ActiveRecord::VERSION::MAJOR < 4 || defined?(::ActiveRecord::MassAssignmentSecurity)
    context "ActiveRecord::Base.send(:attr_accessible, nil)" do
      before do
        Delayed::Backend::ActiveRecord::Job.send(:attr_accessible, nil)
      end

      after do
        Delayed::Backend::ActiveRecord::Job.send(:attr_accessible, *Delayed::Backend::ActiveRecord::Job.new.attributes.keys)
      end

      it "is still accessible" do
        job = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: EnqueueJobMod.new
        expect(Delayed::Backend::ActiveRecord::Job.find(job.id).handler).to_not be_blank
      end
    end
  end

  context "sequential queues" do
    let(:max_runtime) { 2.minutes }
    let(:worker) { Delayed::Worker.new }
    let(:other_worker) do
      other_worker = Delayed::Worker.new
      other_worker.name = 'other_worker'
      other_worker
    end

    before do
      Delayed::Backend::ActiveRecord.configuration.sequential_queue_prefix = 'run_sequentially:'
      Delayed::Backend::ActiveRecord::Job.delete_all
    end

    it "should be disabled by default" do
      configuration = Delayed::Backend::ActiveRecord::Configuration.new
      expect(configuration.sequential_queue_prefix).to be_nil
    end

    it "should work off sequential queues one by one" do
      job_1 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SequentialNamedQueueJob.new
      job_2 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SequentialNamedQueueJob.new

      expect(Delayed::Backend::ActiveRecord::Job.ready_to_run(worker.name, max_runtime).count).to eql(2)
      expect(Delayed::Backend::ActiveRecord::Job.reserve(worker)).to eql(job_1)

      expect(Delayed::Backend::ActiveRecord::Job.ready_to_run(other_worker.name, max_runtime).count).to eql(0)

      worker.run job_1

      expect(Delayed::Backend::ActiveRecord::Job.ready_to_run(worker.name, max_runtime).count).to eql(1)
      expect(Delayed::Backend::ActiveRecord::Job.ready_to_run(other_worker.name, max_runtime).count).to eql(1)
      expect(Delayed::Backend::ActiveRecord::Job.reserve(other_worker)).to eql(job_2)
    end

    it "should run regular jobs in parallel" do
      job_1 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SequentialNamedQueueJob.new
      job_2 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SequentialNamedQueueJob.new

      job_3 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SimpleJob.new

      expect(Delayed::Backend::ActiveRecord::Job.ready_to_run(worker.name, max_runtime).count).to eql(3)
      expect(Delayed::Backend::ActiveRecord::Job.reserve(worker)).to eql(job_1)

      expect(Delayed::Backend::ActiveRecord::Job.ready_to_run(other_worker.name, max_runtime).count).to eql(1)
      expect(Delayed::Backend::ActiveRecord::Job.reserve(other_worker)).to eql(job_3)
    end

    it "should work off different sequential queues in parallel" do
      queue_1_job_1 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SequentialNamedQueueJob.new(queue_name: 'queue_1')
      queue_1_job_2 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SequentialNamedQueueJob.new(queue_name: 'queue_1')
      queue_2_job_1 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SequentialNamedQueueJob.new(queue_name: 'queue_2')
      queue_2_job_2 = Delayed::Backend::ActiveRecord::Job.enqueue payload_object: SequentialNamedQueueJob.new(queue_name: 'queue_3')

      expect(Delayed::Backend::ActiveRecord::Job.ready_to_run(worker.name, max_runtime).count).to eql(4)
      expect(Delayed::Backend::ActiveRecord::Job.reserve(worker)).to eql(queue_1_job_1)

      expect(Delayed::Backend::ActiveRecord::Job.ready_to_run(other_worker.name, max_runtime).count).to eql(2)
      expect(Delayed::Backend::ActiveRecord::Job.reserve(other_worker)).to eql(queue_2_job_1)
    end
  end

  context "ActiveRecord::Base.table_name_prefix" do
    it "when prefix is not set, use 'delayed_jobs' as table name" do
      ::ActiveRecord::Base.table_name_prefix = nil
      Delayed::Backend::ActiveRecord::Job.set_delayed_job_table_name

      expect(Delayed::Backend::ActiveRecord::Job.table_name).to eq "delayed_jobs"
    end

    it "when prefix is set, prepend it before default table name" do
      ::ActiveRecord::Base.table_name_prefix = "custom_"
      Delayed::Backend::ActiveRecord::Job.set_delayed_job_table_name

      expect(Delayed::Backend::ActiveRecord::Job.table_name).to eq "custom_delayed_jobs"

      ::ActiveRecord::Base.table_name_prefix = nil
      Delayed::Backend::ActiveRecord::Job.set_delayed_job_table_name
    end
  end
end
