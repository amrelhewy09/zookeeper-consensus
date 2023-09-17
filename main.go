package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
	"zookeeper/people"
	"zookeeper/zookeeper_lib"

	"github.com/gin-gonic/gin"
	"github.com/go-zookeeper/zk"
)

func main() {
	// instantiate a list of people empty
	peopleList := []people.Person{}
	r := gin.Default()
	domain := flag.String("domain", "", "")
	port := flag.String("port", "", "")
	flag.Parse()

	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*2, zk.WithLogInfo(true))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	zkClient := zookeeper_lib.NewZK(*domain, conn)
	zkClient.RegisterZNode()
	go zkClient.WatchForLiveNodes()
	go zkClient.WatchForElectionNodes()
	syncs := zkClient.SyncFromLeader()
	peopleList = append(peopleList, syncs...)
	log.Println("connected to zookeeper!")

	r.GET("/people", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"data": peopleList})
	})

	r.POST("/people", func(c *gin.Context) {
		// check if request is from leader from request_from header
		var person people.Person
		if err := c.ShouldBindJSON(&person); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		requestOrigin := c.GetHeader("Request-From")
		fmt.Println("request origin: ", requestOrigin)
		if requestOrigin == "leader" {
			// add the person to the list
			peopleList = append(peopleList, person)
			c.Status(http.StatusOK)
			return
		}

		if zkClient.IsLeader() {
			peopleList = append(peopleList, person)
			go zkClient.SendToAllNodes(person)
			c.Status(http.StatusOK)
			return
		} else {
			fmt.Println("not leader, redirecting to leader")
			leader := zkClient.Getleader()
			zookeeper_lib.SendHTTPPostRequest(leader, person, true)
			c.Status(http.StatusPermanentRedirect)
			return
		}
	})

	r.Run(":" + *port)
}
