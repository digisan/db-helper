package mongohelper

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

var (
	Ctx    context.Context
	Cancel context.CancelFunc
	Client *mongo.Client
)

func init() {
	fmt.Println("mongodb helper init...")
	Ctx, Cancel = context.WithCancel(context.Background())
	Client = getMongoClient("", 0)
}
