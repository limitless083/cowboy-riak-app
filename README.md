## cowboy-riak-app
An application which uses cowboy as http server and riak as database to transfer data.It is just a sample.

## run application

1. change dir to transfer, run the command

`rebar3 auto`

2. start transfer application

`application:ensure_all_started(transfer).`

## upload data

`curl -i -X PUT --data-binary @obj_1.txt http://localhost:8080/api/obj/1`

## download data

`curl -X GET http://localhost:8080/api/obj/1 > 1.obj`


