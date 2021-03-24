## Distributed Golang Wordcount Server
### Requirements
1. Go ```v1.16```
2. Python3
### Steps to run:
1. Server Setup:
    1. Inside each folder i.e Leadership,Leadership2,Leadership3,Leadership4
    2. Run ``` go run main.go ```<br>
Note: By default Leadership,Leadership2,Leadership3,Leadership4 will run on port 8000,8001,8002,8003 respectively. <br>
2. Client Setup:
    1. Run ```chmod +x main.py```
    2. Run ```./main.py -p <port> -host <hostname> -f <file .txt> ``` or ```./main.py -h``` to see the help menu <br>
    For example: ```./main.py -p 8004 -host localhost -f harrypotter.txt ```


### Creating more servers
Essentially ```main.go``` in leadership[n] folders is the same. Just change the port number while running it. So it can run as a seperate process.





