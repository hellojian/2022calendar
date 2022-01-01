package main

import (
	middlewares "awesomeProject3/middleware"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"gopkg.in/mgo.v2/bson"
	"os"
	"strings"
)

type Todo struct {
	Id   bson.ObjectId `json:"id" bson:"_id"`
	Info string        `json:"info" bson:"info"`
	//Status string        `json:"status" bson:"status"`
	Month string `json:"month" bson:"month"`
	Day   string `json:"day" bson:"day"`
}

type Req struct {
	Todos []Todo `json:"todos"`
}

func main() {
	//1.创建路由
	r := gin.Default()
/*
	r.Static("/static", "./static")
	r.GET("/", func(context *gin.Context) {
		t, _ := template.ParseFiles("static/index.html")
		t.Execute(context.Writer, nil)
	})*/

	//2.绑定路由规则，执行的函数
	r.Use(middlewares.Cors())
	r.GET("/getTodos/month/:month", func(context *gin.Context) {
		session := Get(context)
		defer session.Close()
		todos := []Todo{}
		month := context.Param("month")
		session.FindAll("todo", bson.M{"month": month}, &todos)

		context.JSON(200, gin.H{"todos": todos})
	})
	r.GET("/getTodos/day/:date", func(context *gin.Context) {
		session := Get(context)
		defer session.Close()
		todos := []Todo{}
		month := context.Param("date")
		session.FindAll("todo", bson.M{"day": month}, &todos)

		context.JSON(200, gin.H{"todos": todos})
	})
	r.POST("/updateTodos/day/:date", func(context *gin.Context) {
		session := Get(context)
		defer session.Close()
		date := context.Param("date")
		data := context.PostForm("data")
		var requset Req

		fmt.Printf(data)

		if err := json.Unmarshal([]byte(data), &requset); err == nil {
			fmt.Println(requset)
		}

		session.Delete("todo", bson.M{"day": date})

		for _, todo := range requset.Todos {
			todo.Id = bson.NewObjectId()
			todo.Month = strings.Replace(date, "-", "", -1)[0:6]
			todo.Day = date
			if err := session.Insert("todo", todo); err != nil {
				fmt.Print(err)
			}

		}

		context.JSON(200, gin.H{"error": nil})
	})
	//3.监听端口，默认8080
	r.Run(":8888")
	for {
	}
	fmt.Fprintln(os.Stderr, "\nProcess Ended Successfully.")
}
