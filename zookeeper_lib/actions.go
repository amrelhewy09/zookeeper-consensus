package zookeeper_lib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"zookeeper/people"

	"github.com/go-zookeeper/zk"
)

const ALL_NODE_PATH = "/all"
const LIVE_NODE_PATH = "/live"
const ELECTION_NODE_PATH = "/election"

type ZkClient struct {
	domain string
	conn   *zk.Conn
	leader string
}

func NewZK(domain string, conn *zk.Conn) *ZkClient {
	return &ZkClient{
		domain: domain,
		conn:   conn,
	}
}

func (c *ZkClient) RegisterZNode() {
	fmt.Println(ALL_NODE_PATH + "/" + c.domain)

	_, err := c.conn.Create(ALL_NODE_PATH+"/"+c.domain, []byte(c.domain), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		panic(err)
	}
	// create live node
	_, err = c.conn.Create(LIVE_NODE_PATH+"/"+c.domain, []byte(c.domain), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}

	_, err = c.conn.Create(ELECTION_NODE_PATH+"/leader", []byte(c.domain), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))

	if err != nil {
		panic(err)
	}

	fmt.Println("registered znode with domain name: ", c.domain)
}

func (c *ZkClient) WatchForLiveNodes() {
	// watch for live nodes
	liveNodes, _, liveNodeEvents, err := c.conn.ChildrenW(LIVE_NODE_PATH)
	if err != nil {
		panic(err)
	}
	fmt.Println("live nodes: ", liveNodes)
	for {
		select {
		case event := <-liveNodeEvents:
			fmt.Println("event received: ", event)
			if event.Type == zk.EventNodeChildrenChanged {
				fmt.Println("live nodes changed")
				liveNodes, _, liveNodeEvents, err = c.conn.ChildrenW(LIVE_NODE_PATH)
				if err != nil {
					panic(err)
				}
				fmt.Println("live nodes: ", liveNodes)
			}
		}
	}
}

func (c *ZkClient) WatchForElectionNodes() {
	// watch for live nodes
	electionNodes, _, electionNodeEvents, err := c.conn.ChildrenW(ELECTION_NODE_PATH)
	if err != nil {
		panic(err)
	}
	if electionNodes == nil {
		panic("election nodes is nil")
	}
	fmt.Println("election nodes: ", electionNodes)
	c.leader = electNewLeader(electionNodes, c)
	for {
		select {
		case event := <-electionNodeEvents:
			fmt.Println("event received: ", event)
			if event.Type == zk.EventNodeChildrenChanged {
				fmt.Println("election nodes changed")
				electionNodes, _, electionNodeEvents, err = c.conn.ChildrenW(ELECTION_NODE_PATH)
				if err != nil {
					panic(err)
				}
				c.leader = electNewLeader(electionNodes, c)
			}
		}
	}
}

func (c *ZkClient) IsLeader() bool {
	// check if current node is leader
	if c.domain == c.leader {
		return true
	} else {
		return false
	}
}
func (c *ZkClient) Getleader() string {
	return c.leader
}

func (c *ZkClient) SendToAllNodes(person people.Person) {
	allNodes, _, err := c.conn.Children(LIVE_NODE_PATH)
	if err != nil {
		panic(err)
	}
	for _, node := range allNodes {
		if node != c.domain {
			fmt.Println("sending to node: ", node)
			if !SendHTTPPostRequest(node, person, false) {
				fmt.Println("failed to send to node: ", node)
			}
		}
	}
}

func electNewLeader(electionNodes []string, c *ZkClient) string {
	sort.Strings(electionNodes)

	leader := electionNodes[0]
	data, _, _ := c.conn.Get(ELECTION_NODE_PATH + "/" + leader)
	fmt.Println("leader is: ", string(data))
	return string(data)
}

func SendHTTPPostRequest(node string, person people.Person, leader bool) bool {
	url := "http://" + node + "/people"
	req, err := http.NewRequest("POST", url, person.ToJSON())
	if err != nil {
		fmt.Println(err)
	}
	if !leader {
		req.Header.Set("Request-From", "leader")
	}
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func (c *ZkClient) SyncFromLeader() []people.Person {
	leader := c.getLeaderFromChildren()
	if leader == "" || leader == c.domain {
		return []people.Person{}
	}

	// send get request to leader
	url := "http://" + leader + "/people"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println(err)
	}
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()

	respData := make(map[string][]people.Person)

	err = json.NewDecoder(resp.Body).Decode(&respData)
	if err != nil {
		fmt.Println(err)
	}
	peopleList := respData["data"]
	fmt.Println("synced from leader: ", peopleList)

	return peopleList
}

func (c *ZkClient) getLeaderFromChildren() string {
	children, _, err := c.conn.Children(ELECTION_NODE_PATH)
	if err != nil {
		panic(err)
	}
	// sort the children
	sort.Strings(children)
	// get the leader
	leader := children[0]
	data, _, _ := c.conn.Get(ELECTION_NODE_PATH + "/" + leader)
	return string(data)
}
