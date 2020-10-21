```puml
@startuml component
package onprem {
    database mysql
    node vtgate
}
package cloud {
    node cloner
    database tidb
}

cloner -> vtgate : grpc over square-envoy
vtgate -> mysql
cloner -> tidb

@enduml
```
