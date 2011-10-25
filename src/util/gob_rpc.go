package waffle

import (
	"gob"
	"http"
	"net"
	"os"
	"rpc"
	"sync"
)

// GobRpc will ship with the base distribution
type GobRpcClientBase struct {
	lock          sync.Mutex
	workerClients map[string]*rpc.Client
}

func (c *GobRpcClientBase) init() {
	c.workerClients = make(map[string]*rpc.Client)

	// Register the base types with gob
	gob.Register(&VertexBase{})
	gob.Register(&EdgeBase{})
	gob.Register(&MsgBase{})
}

type GobMasterRpcClient struct {
	GobRpcClientBase
}

func (s *GobMasterRpcClient) Init() {
	// For ClientBase
	s.GobRpcClientBase.init()
}

func (c *GobMasterRpcClient) workerClient(workerAddr string) (*rpc.Client, os.Error) {
	c.lock.Lock()
	if _, ok := c.workerClients[workerAddr]; !ok {
		cl, err := rpc.DialHTTP("tcp", workerAddr)
		if err != nil {
			return nil, err
		}
		c.workerClients[workerAddr] = cl
	}
	client := c.workerClients[workerAddr]
	c.lock.Unlock()
	return client, nil
}

func (s *GobMasterRpcClient) PushTopology(workerAddr string, info *TopologyInfo) os.Error {
	client, err := s.workerClient(workerAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err = client.Call("GobWorkerRpcServer.RecieveTopology", info, &resp); err != nil {
		return err
	}
	return nil
}

func (s *GobMasterRpcClient) ExecutePhase(workerAddr string, exec *PhaseExec) os.Error {
	client, err := s.workerClient(workerAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err = client.Call("GobWorkerRpcServer.ExecPhase", exec, &resp); err != nil {
		return err
	}
	return nil
}

type GobWorkerRpcClient struct {
	GobRpcClientBase
	mclient *rpc.Client
}

func (c *GobWorkerRpcClient) Init() {
	// For ClientBase
	c.GobRpcClientBase.init()
}

func (c *GobWorkerRpcClient) workerClient(workerAddr string) (*rpc.Client, os.Error) {
	// TODO avoid lock contention, clean up the node interface so we can access wmap cleanly
	c.lock.Lock()
	if _, ok := c.workerClients[workerAddr]; !ok {
		cl, err := rpc.DialHTTP("tcp", workerAddr)
		if err != nil {
			return nil, err
		}
		c.workerClients[workerAddr] = cl
	}
	client := c.workerClients[workerAddr]
	c.lock.Unlock()
	return client, nil
}

func (c *GobWorkerRpcClient) masterClient(masterAddr string) (*rpc.Client, os.Error) {
	if c.mclient == nil {
		client, err := rpc.DialHTTP("tcp", masterAddr)
		if err != nil {
			return nil, err
		}
		c.mclient = client
	}
	return c.mclient, nil
}

func (c *GobWorkerRpcClient) Register(masterAddr, host, port string) (string, string, os.Error) {
	mclient, err := c.masterClient(masterAddr)
	if err != nil {
		return "", "", err
	}
	var resp RegisterResp
	if err = mclient.Call("GobMasterRpcServer.RegisterWorker", &RegisterInfo{Addr: host, Port: port}, &resp); err != nil {
		return "", "", err
	}
	return resp.WorkerId, resp.JobId, nil
}

func (c *GobWorkerRpcClient) SendSummary(masterAddr string, summary *PhaseSummary) os.Error {
	mclient, err := c.masterClient(masterAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err := mclient.Call("GobMasterRpcServer.EnterPhaseBarrier", summary, &resp); err != nil {
		return err
	}
	return nil
}

func (c *GobWorkerRpcClient) SendVertices(workerAddr string, vertices []Vertex) os.Error {
	client, err := c.workerClient(workerAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err = client.Call("GobWorkerRpcServer.QueueVertices", vertices, &resp); err != nil {
		return err
	}
	return nil
}

func (c *GobWorkerRpcClient) SendMessages(workerAddr string, msgs []Msg) os.Error {
	client, err := c.workerClient(workerAddr)
	if err != nil {
		return err
	}
	var resp Resp
	if err = client.Call("GobWorkerRpcServer.QueueMessages", msgs, &resp); err != nil {
		return err
	}
	return nil
}

type GobMasterRpcServer struct {
	master *Master
}

func (s *GobMasterRpcServer) Start(master *Master) {
	s.master = master
	rpc.Register(s)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", net.JoinHostPort(s.master.Host(), s.master.Port()))
	if err != nil {
		panic(err)
	}
	go http.Serve(listener, nil)
}

func (s *GobMasterRpcServer) RegisterWorker(info *RegisterInfo, resp *RegisterResp) os.Error {
	workerId, jobId, err := s.master.RegisterWorker(info.Addr, info.Port)
	if err != nil {
		return err
	}
	resp.WorkerId, resp.JobId = workerId, jobId
	return nil
}

func (s *GobMasterRpcServer) EnterPhaseBarrier(summary *PhaseSummary, resp *Resp) os.Error {
	*resp = OK
	s.master.EnterBarrier(summary)
	return nil
}

func (s *GobWorkerRpcServer) Start(worker *Worker) {
	s.worker = worker
	rpc.Register(s)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", net.JoinHostPort(s.worker.Host(), s.worker.Port()))
	if err != nil {
		panic(err)
	}
	go http.Serve(listener, nil)
}

type GobWorkerRpcServer struct {
	worker *Worker
}

func (s *GobWorkerRpcServer) ExecPhase(exec *PhaseExec, resp *Resp) os.Error {
	*resp = OK
	return s.worker.ExecPhase(exec)
}

func (s *GobWorkerRpcServer) RecieveTopology(info *TopologyInfo, resp *Resp) os.Error {
	*resp = OK
	s.worker.SetTopology(info.WorkerMap, info.PartitionMap)
	return nil
}

func (s *GobWorkerRpcServer) QueueMessages(msgs []Msg, resp *Resp) os.Error {
	*resp = OK
	s.worker.QueueMessages(msgs)
	return nil
}

func (s *GobWorkerRpcServer) QueueVertices(vertices []Vertex, resp *Resp) os.Error {
	*resp = OK
	s.worker.QueueVertices(vertices)
	return nil
}
