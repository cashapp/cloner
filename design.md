```puml
@startuml component
package onprem {
    node vtgate
    package ods {
        node vttablet
        database mysql
    }
}
package cloud {
    node cloner
    database tidb
}

cloner -> vtgate : grpc over square-envoy
vtgate -> vttablet
vttablet -> mysql
cloner -> tidb

@enduml
```
