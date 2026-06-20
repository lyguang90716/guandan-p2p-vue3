# 阶段一：使用 Node 镜像进行编译构建
FROM node:18-alpine AS build-stage
WORKDIR /app

# 复制依赖配置并安装
COPY package*.json ./
RUN npm install

# 复制所有源码并执行打包
COPY . .
RUN npm run build

# 阶段二：使用 Nginx 镜像来托管打包后的静态网页
FROM nginx:stable-alpine AS production-stage
# 将阶段一编译好的静态文件复制到 Nginx 目录下
COPY --from=build-stage /app/dist /usr/share/nginx/html

# 显式声明容器提供服务的端口为 80
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
