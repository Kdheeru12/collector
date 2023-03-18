package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"google.golang.org/grpc/credentials"

	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	serviceName  = os.Getenv("SERVICE_NAME")
	collectorURL = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	insecure     = os.Getenv("INSECURE_MODE")
)

func GetData(c *gin.Context) {
	res, err := http.Get("https://dummyjson.com/products")
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	c.String(http.StatusOK, string(body))
}

func GetData2(c *gin.Context) {
	res, err := http.Get("https://api.example.com/data")
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	c.String(http.StatusOK, string(body))
}

func initTracer() func(context.Context) error {

    secureOption := otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, ""))
    if len(insecure) > 0 {
        secureOption = otlptracegrpc.WithInsecure()
    }

    exporter, err := otlptrace.New(
        context.Background(),
        otlptracegrpc.NewClient(
            secureOption,
            otlptracegrpc.WithEndpoint(collectorURL),
        ),
    )

    if err != nil {
        log.Fatal(err)
    }
    resources, err := resource.New(
        context.Background(),
        resource.WithAttributes(
            attribute.String("service.name", serviceName),
            attribute.String("library.language", "go"),
        ),
    )
    if err != nil {
        log.Printf("Could not set resources: ", err)
    }

    otel.SetTracerProvider(
        sdktrace.NewTracerProvider(
            sdktrace.WithSampler(sdktrace.AlwaysSample()),
            sdktrace.WithBatcher(exporter),
            sdktrace.WithResource(resources),
        ),
    )
    return exporter.Shutdown
}

func main() {
	cleanup := initTracer()
    defer cleanup(context.Background())

	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/mishipay")
	if err != nil {
		fmt.Println(err)
		panic("failed to connect database")
		
	}
	defer db.Close()

	r := gin.Default()
    r.Use(otelgin.Middleware(serviceName))

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "ong",
		})
	})
	r.GET("/dddk", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "ong",
		})
	})

	r.GET("/users", func(c *gin.Context) {
		insert := "INSERT INTO users (name) VALUES (?)"
		name := "User " + strconv.Itoa(rand.Intn(10000))
		if _, err := db.Exec(insert, name); err != nil {
			fmt.Print(err)
			c.JSON(500, gin.H{
				"message": "failed to insert user",
			})
			return
		}

		rows, err := db.Query("SELECT name FROM users")
		if err != nil {
			c.JSON(500, gin.H{
				"message": "failed to query users",
			})
			return
		}
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				c.JSON(500, gin.H{
					"message": "failed to scan user",
				})
				return
			}
			names = append(names, name)
		}
		c.JSON(200, gin.H{
			"names": names,
		})
	})

	r.GET("/data", GetData)
	r.GET("/data2", GetData2)

	r.Run()
}
