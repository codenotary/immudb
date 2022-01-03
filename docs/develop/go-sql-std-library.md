# Go SQL STD library

### GO SQL std library <a href="#go-sql-std-library" id="go-sql-std-library"></a>

From immudb `v1.1.0` is possible to use go standard library sql interface to query data.

```
import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/stdlib"
)
```

```
opts := client.DefaultOptions()
opts.Username = "immudb"
opts.Password = "immudb"
opts.Database = "defaultdb"

db := stdlib.OpenDB(opts)
defer db.Close()

_, err := db.ExecContext(context.TODO(), "CREATE TABLE myTable(id INTEGER, name VARCHAR, PRIMARY KEY id)")
_, err = db.ExecContext(context.TODO(), "INSERT INTO myTable (id, name) VALUES (1, 'immu1')")
rows, err := db.QueryContext(context.TODO(), "SELECT * FROM myTable")

var id uint64
var name string
defer rows.Close()
rows.Next()
err = rows.Scan(&id, &name)
if err != nil {
panic(err)
}

fmt.Printf("id: %d\n", id)
fmt.Printf("name: %s\n", name)
```

In alternative is possible to open immudb with a connection string:

```
import (
	"database/sql"
	_ "github.com/codenotary/immudb/pkg/stdlib"
)

func main() {
	db, err := sql.Open("immudb", "immudb://immudb:immudb@127.0.0.1:3322/defaultdb?sslmode=disable")
	defer db.Close()
```

Available SSL modes are:

* **disable**. SSL is off
* **insecure-verify**. SSL is on but client will not check the server name.
* **require**. SSL is on.
