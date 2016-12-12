defmodule Verk.QueueStatsTest do
  use ExUnit.Case
  import Verk.QueueStats

  @table :queue_stats

  setup do
    { :ok, _ } = Application.fetch_env(:verk, :redis_url)
                  |> elem(1)
                  |> Redix.start_link([name: Verk.Redis])
    Redix.pipeline!(Verk.Redis, [["DEL",
        "stat:failed", "stat:processed",
        "stat:failed:queue_1", "stat:processed:queue_1",
        "stat:failed:queue_2", "stat:processed:queue_2"
      ]])
    :ok
  end

  describe "all/1" do
    test "list counters" do
      init([]) # create table

      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_1" } }, :state)
      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_1" } }, :state)
      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_2" } }, :state)
      handle_event(%Verk.Events.JobFinished{ job: %Verk.Job{ queue: "queue_1" } }, :state)
      handle_event(%Verk.Events.JobFailed{ job: %Verk.Job{ queue: "queue_1" } }, :state)

      assert all == [%{ queue: "queue_1", running_counter: 0, finished_counter: 1, failed_counter: 1 },
                     %{ queue: "queue_2", running_counter: 1, finished_counter: 0, failed_counter: 0 } ]
    end

    test "list counters searching for a prefix" do
      init([]) # create table

      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "default" } }, :state)
      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "default" } }, :state)
      handle_event(%Verk.Events.JobFinished{ job: %Verk.Job{ queue: "default" } }, :state)
      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "default-something" } }, :state)
      handle_event(%Verk.Events.JobFailed{ job: %Verk.Job{ queue: "default-something" } }, :state)
      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "priority" } }, :state)

      assert all("def") == [%{ queue: "default", running_counter: 1, finished_counter: 1, failed_counter: 0 },
                            %{ queue: "default-something", running_counter: 0, finished_counter: 0, failed_counter: 1 } ]
    end
  end

  describe "handle_call/2" do
    test "reset_started with no element" do
      init([]) # create table

      assert handle_call({ :reset_started, "queue" }, :state) == { :ok, :ok, :state }
      assert :ets.tab2list(@table) == [{ 'queue', 0, 0, 0, 0, 0 }]
    end

    test "reset_started with existing element" do
      init([]) # create table
      :ets.insert_new(@table, { 'queue', 1, 2, 3, 4, 5 })

      assert handle_call({ :reset_started, "queue" }, :state) == { :ok, :ok, :state }
      assert :ets.tab2list(@table) == [{ 'queue', 0, 2, 3, 4, 5 }]
    end
  end

  describe "init/1" do
    test "creates an ETS table" do
      assert :ets.info(@table) == :undefined

      assert init([]) == { :ok, nil }

      assert :ets.info(@table) != :undefined
    end
  end

  describe "handle_event/2" do
    test "with started event" do
      init([]) # create table
      event = %Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue" } }

      assert handle_event(event, :state) == { :ok, :state }

      assert :ets.tab2list(@table) == [{ :total, 1, 0, 0, 0, 0 }, { 'queue', 1, 0, 0, 0, 0 }]
    end

    test "with finished event" do
      init([]) # create table
      event = %Verk.Events.JobFinished{ job: %Verk.Job{ queue: "queue" } }

      assert handle_event(event, :state) == { :ok, :state }

      assert :ets.tab2list(@table) == [{ :total, -1, 1, 0, 0, 0 }, { 'queue', -1, 1, 0, 0, 0 }]
    end

    test "with failed event" do
      init([]) # create table
      event = %Verk.Events.JobFailed{ job: %Verk.Job{ queue: "queue" } }

      assert handle_event(event, :state) == { :ok, :state }

      assert :ets.tab2list(@table) == [{ :total, -1, 0, 1, 0, 0 }, { 'queue', -1, 0, 1, 0, 0 }]
    end
  end

  describe "handle_info/2" do
    test "persist processed and failed counts" do
      init([])

      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_1" } }, :state)
      handle_event(%Verk.Events.JobFailed{ job: %Verk.Job{ queue: "queue_1" } }, :state)
      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_2" } }, :state)
      handle_event(%Verk.Events.JobFinished{ job: %Verk.Job{ queue: "queue_2" } }, :state)

      assert handle_info(:persist_stats, :state) == {:ok, :state}

      result = Redix.command!(Verk.Redis, ["MGET", "stat:processed:queue_1", "stat:failed:queue_1",
                                                   "stat:processed:queue_2", "stat:failed:queue_2",
                                                   "stat:processed", "stat:failed"])
      assert result == [
        nil, "1",
        "1", nil,
        "1", "1"
      ]

      handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_1" } }, :state)
      handle_event(%Verk.Events.JobFinished{ job: %Verk.Job{ queue: "queue_1" } }, :state)
      assert handle_info(:persist_stats, :state) == { :ok, :state }
      result = Redix.command!(Verk.Redis, ["MGET", "stat:processed:queue_1", "stat:failed:queue_1",
                                                   "stat:processed:queue_2", "stat:failed:queue_2",
                                                   "stat:processed", "stat:failed"])
      assert result == [
        "1", "1",
        "1", nil,
        "2", "1"
      ]

      assert handle_info(:persist_stats, :state) == { :ok, :state }

      result = Redix.command!(Verk.Redis, ["MGET", "stat:processed:queue_1", "stat:failed:queue_1",
                                                   "stat:processed:queue_2", "stat:failed:queue_2",
                                                   "stat:processed", "stat:failed"])
      assert result == [
        "1", "1",
        "1", nil,
        "2", "1"
      ]
    end
  end
end
