defmodule Swarm.QuorumWithoutRespawnTests do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  @node1 :"node11@127.0.0.1"
  @node2 :"node12@127.0.0.1"
  @node3 :"node13@127.0.0.1"
  @node4 :"node14@127.0.0.1"
  @node5 :"node15@127.0.0.1"
  @nodes [@node1, @node2, @node3, @node4, @node5]
  @names [{:test, 1}, {:test, 2}, {:test, 3}, {:test, 10}, {:test, 17}]

  alias Swarm.Cluster
  alias Swarm.Distribution.{Ring, StaticQuorumRing}

  setup_all do
    :rand.seed(:exs64)

    Application.put_env(:swarm, :static_quorum_size, 3)
    Application.put_env(:swarm, :respawn_on_node_down, false)
    restart_cluster_using_strategy(StaticQuorumRing, [])

    MyApp.WorkerSup.start_link()

    on_exit(fn ->
      Application.delete_env(:swarm, :static_quorum_size)
      Application.delete_env(:swarm, :respawn_on_node_down)

      nodes = Application.get_env(:swarm, :nodes, [])
      restart_cluster_using_strategy(Ring, nodes)
    end)

    :ok
  end

  setup do
    on_exit(fn ->
      # stop any started nodes after each test
      @nodes
      |> Enum.map(&Task.async(fn -> Cluster.stop_node(&1) end))
      |> Enum.map(&Task.await(&1, 30_000))
    end)
  end

  describe "net split" do
    setup [:form_five_node_cluster]

    test "should have process on each node before it happens" do
      # start worker for each name
      Enum.each(@names, fn name ->
        {:ok, _pid} = register_name(@node1, name, MyApp.WorkerSup, :register, [])
      end)

      nodes_without_pids =
        @names
        |> Enum.map(&whereis_name(@node1, &1))
        |> Enum.reduce(@nodes, fn pid, nodes -> List.delete(nodes, node(pid)) end)

      assert nodes_without_pids == []
    end

    test "for respawn_on_node_down: false should leave cluster without processes from smaller partition" do
      # start worker for each name
      Enum.each(@names, fn name ->
        {:ok, _pid} = register_name(@node1, name, MyApp.WorkerSup, :register, [])
      end)

      names_on_smaller_partition =
        @names
        |> Enum.map(&whereis_name(@node1, &1))
        |> Enum.filter(fn pid -> node(pid) in [@node4, @node5] end)

      assert Enum.count(names_on_smaller_partition) == 2

      :timer.sleep(1_000)

      # simulate net split (1, 2, 3) and (4, 5)
      simulate_disconnect([@node1, @node2, @node3], [@node4, @node5])

      :timer.sleep(1_000)

      # ensure processes from smaller partition are not alive
      names_on_smaller_partition
      |> Enum.map(&whereis_name(@node1, &1))
      |> Enum.each(fn pid ->
        assert pid == :undefined
      end)
    end

    # simulate a disconnect between the two node partitions
    defp simulate_disconnect(lpartition, rpartition) do
      Enum.each(lpartition, fn lnode ->
        Enum.each(rpartition, fn rnode ->
          send({Swarm.Tracker, lnode}, {:nodedown, rnode, nil})
          send({Swarm.Tracker, rnode}, {:nodedown, lnode, nil})
        end)
      end)
    end
  end

  defp register_name(node, name, m, f, a, timeout \\ :infinity)

  defp register_name(node, name, m, f, a, timeout) do
    case :rpc.call(node, Swarm, :register_name, [name, m, f, a, timeout], :infinity) do
      {:badrpc, reason} -> {:error, reason}
      reply -> reply
    end
  end

  defp whereis_name(node, name) do
    :rpc.call(node, Swarm, :whereis_name, [name], :infinity)
  end

  defp form_five_node_cluster(_context) do
    with {:ok, _node1} <- Cluster.spawn_node(@node1),
         {:ok, _node2} <- Cluster.spawn_node(@node2),
         {:ok, _node3} <- Cluster.spawn_node(@node3),
         {:ok, _node4} <- Cluster.spawn_node(@node4),
         {:ok, _node5} <- Cluster.spawn_node(@node5) do
      Process.sleep(2000)
      :ok
    end
  end

  # start worker for each name
  def start_named_processes do
    Enum.map(@names, fn name ->
      with {:ok, pid} <- register_name(@node1, name, MyApp.WorkerSup, :register, []) do
        Process.monitor(pid)
      end
    end)
  end

  defp restart_cluster_using_strategy(strategy, nodes) do
    Cluster.stop()

    Application.put_env(:swarm, :distribution_strategy, strategy)
    Application.stop(:swarm)

    Cluster.spawn(nodes)

    Application.ensure_all_started(:swarm)
  end
end
