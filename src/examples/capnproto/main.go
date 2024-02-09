package main

import (
    "fmt"
    "io/ioutil"
    "os"

    "capnp.org/capnp/v3_0/capnp"
    "/home/bill/GolandProjects/backups-done-right/src/examples/capnproto/car.capnp"
)

func main() {
    // Create a new Car.
    msg, seg, _ := capnp.NewMessage(capnp.SingleSegment(nil))
    car, _ := car.NewRootCar(seg)
    car.SetMake("Ford")
    car.SetModel("Mustang")

    // Write the car data to a file.
    file, _ := os.Create("carData")
    _ = capnp.NewEncoder(file).Encode(msg)
    file.Close()

    // Read the car data from the file.
    file, _ = os.Open("carData")
    data, _ := ioutil.ReadAll(file)
    file.Close()

    // Decode the car data.
    msg, _ = capnp.NewDecoder(bytes.NewReader(data)).Decode()
    car, _ = car.ReadRootCar(msg)
    
    fmt.Println("Make: ", car.Make())
    fmt.Println("Model: ", car.Model())
}
