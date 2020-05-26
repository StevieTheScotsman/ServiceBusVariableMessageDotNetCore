This is a dot net core program that will determine if a blob reference will be used to send a message on the service bus.
If less than 256K which is the standard size then it will be sent on the wire otherwise a blob reference will be sent.
