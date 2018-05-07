defmodule Verk.NodeTest do
  use ExUnit.Case
  import Verk.Node

  @verk_nodes_key "verk_nodes"

  @node "123"
  @node_key "verk:node:123"
  @node_queues_key "verk:node:123:queues"

  setup do
    {:ok, redis} = :verk |> Confex.get_env(:redis_url) |> Redix.start_link
    Redix.command!(redis, ["DEL", @verk_nodes_key, @node_queues_key])
    {:ok, %{redis: redis}}
  end

  defp register(%{redis: redis}), do: register(@node, 555, redis)
  defp add_queues(%{redis: redis}) do
    add_queue(@node, "queue_1", redis)
    add_queue(@node, "queue_2", redis)
    :ok
  end

  describe "register/3" do
    test "add to verk_nodes", %{redis: redis} do
      assert register(@node, 555, redis) == :ok
      assert Redix.command!(redis, ["SMEMBERS", "verk_nodes"]) == ["123"]
      assert register(@node, 555, redis) == {:error, :node_id_already_running}
    end

    test "set verk:node: key", %{redis: redis} do
      assert register(@node, 555, redis) == :ok
      assert Redix.command!(redis, ["GET", @node_key]) == "alive"
    end

    test "expire verk:node: key", %{redis: redis} do
      assert register(@node, 555, redis) == :ok
      ttl = Redix.command!(redis, ["PTTL", @node_key])
      assert_in_delta ttl, 555, 5
    end
  end

  describe "deregister/2" do
    setup :register

    test "remove from verk_nodes", %{redis: redis} do
      assert Redix.command!(redis, ["SMEMBERS", "verk_nodes"]) == ["123"]
      assert deregister!(@node, redis) == :ok
      assert Redix.command!(redis, ["SMEMBERS", "verk_nodes"]) == []
    end

    test "remove verk:node: key", %{redis: redis} do
      assert Redix.command!(redis, ["GET", @node_key]) == "alive"
      assert deregister!(@node, redis) == :ok
      assert Redix.command!(redis, ["GET", @node_key]) == nil
    end

    test "remove verk:node::queues key", %{redis: redis} do
      assert Redix.command!(redis, ["SADD", @node_queues_key, "queue_1"]) == 1
      assert deregister!(@node, redis) == :ok
      assert Redix.command!(redis, ["GET", @node_queues_key]) == nil
    end
  end

  describe "members/3" do
    setup :register

    test "list verk nodes", %{redis: redis} do
      assert members(redis) == {:ok, [@node]}
    end
  end

  describe "ttl!/2" do
    setup :register

    test "return ttl from a verk node id", %{redis: redis} do
      assert_in_delta ttl!(@node, redis), 555, 5
    end
  end

  describe "expire_in/3" do
    test "resets expiration item", %{redis: redis} do
      assert expire_in(@node, 888, redis)
      assert_in_delta Redix.command!(redis, ["PTTL", @node_key]), 888, 5
    end
  end

  describe "queues!/2" do
    setup :add_queues

    test "list queues", %{redis: redis} do
      queues = MapSet.new(queues!(@node, redis))
      assert MapSet.equal?(queues, MapSet.new(["queue_1", "queue_2"]))
    end
  end

  describe "add_queue/3" do
    test "add queue to verk:node::queues", %{redis: redis} do
      queue_name = "default"
      assert add_queue(@node, queue_name, redis)
      assert Redix.command!(redis, ["SMEMBERS", @node_queues_key]) == [queue_name]
    end
  end

  describe "remove_queue/3" do
    test "remove queue from verk:node::queues", %{redis: redis} do
      queue_name = "default"
      assert add_queue(@node, queue_name, redis)
      assert Redix.command!(redis, ["SMEMBERS", @node_queues_key]) == [queue_name]
    end
  end
end
