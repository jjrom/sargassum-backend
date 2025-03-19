FROM node:23.9.0-slim

# Set environment variables to avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python, build tools, and other dependencies for DuckDB
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    build-essential \
    gcc \
    g++ \
    make \
    && apt-get clean
# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY package.json ./

RUN npm install

# Bundle app source
COPY server.js .

RUN mkdir /data
RUN mkdir /cache

EXPOSE 3001
CMD [ "npm", "start" ]