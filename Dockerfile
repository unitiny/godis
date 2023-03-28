FROM golang

# 为我们的镜像设置必要的环境变量
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
	GOPROXY="https://goproxy.cn,direct"

WORKDIR /home/godis

# 将代码复制到容器中
COPY . .

# 将我们的代码编译成二进制可执行文件  可执行文件名为
# RUN go build -o app .

# 声明服务端口
EXPOSE 6379

# 启动容器时运行的命令
# CMD ["/home/godis/app"]
