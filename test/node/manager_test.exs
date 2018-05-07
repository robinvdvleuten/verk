defmodule Verk.Node.ManagerTest do
  use ExUnit.Case
  alias Verk.InProgressQueue
  import Verk.Node.Manager
  import :meck

  @verk_node_id "node-manager-test"
  @frequency 10

  setup do
    new [Verk.Node, InProgressQueue]
    Application.put_env(:verk, :local_node_id, @verk_node_id)
    Application.put_env(:verk, :heartbeat, @frequency)

    on_exit fn ->
      Application.delete_env(:verk, :local_node_id)
      Application.delete_env(:verk, :heartbeat)
      unload()
    end

    :ok
  end

  describe "init/1" do
    test "registers local verk node id" do
      expect(Verk.Node, :register, [@verk_node_id, 2 * @frequency, Verk.Redis], :ok)

      assert init([]) == {:ok, {@verk_node_id, @frequency}}
      assert_receive :heartbeat
    end
  end

  describe "handle_info/2" do
    test "heartbeat when only one node" do
      state = {@verk_node_id, @frequency}
      expect(Verk.Node, :members, [0, Verk.Redis], {:ok, [@verk_node_id]})
      expect(Verk.Node, :expire_in, [@verk_node_id, 2 * @frequency, Verk.Redis], :ok)
      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end

    test "heartbeat when more than 1 node - alive" do
      alive_node_id = "alive-node"
      state = {@verk_node_id, @frequency}
      expect(Verk.Node, :members, [0, Verk.Redis], {:ok, [@verk_node_id, alive_node_id]})
      expect(Verk.Node, :ttl!, [alive_node_id, Verk.Redis], 500)
      expect(Verk.Node, :expire_in, [@verk_node_id, 2 * @frequency, Verk.Redis], :ok)
      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end

    test "heartbeat when more than 1 node - dead" do
      dead_node_id = "dead-node"
      state = {@verk_node_id, @frequency}
      queues = ["queue_1"]

      expect(Verk.Node, :members, [0, Verk.Redis], {:ok, [@verk_node_id, dead_node_id]})
      expect(Verk.Node, :ttl!, [dead_node_id, Verk.Redis], -2)
      expect(Verk.Node, :expire_in, [@verk_node_id, 2 * @frequency, Verk.Redis], :ok)
      expect(Verk.Node, :queues!, ["dead-node", Verk.Redis], queues)
      expect(InProgressQueue, :enqueue_in_progress, ["queue_1", dead_node_id, Verk.Redis], {:ok, [0, 1]})
      expect(Verk.Node, :deregister!, [Verk.Redis, dead_node_id], :ok)

      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end
  end
end
