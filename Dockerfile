#FROM ubuntu
#RUN apt-get update
#RUN apt-get install -y git nodejs npm
#RUN git clone https://github.com/DuoSoftware/DVP-CDREngine.git /usr/local/src/cdrengine
#RUN cd /usr/local/src/cdrengine; npm install
#CMD ["nodejs", "/usr/local/src/cdrengine/app.js"]

#EXPOSE 8816

# FROM node:9.9.0
# ARG VERSION_TAG
# RUN git clone -b $VERSION_TAG https://github.com/DuoSoftware/DVP-CDREngine.git /usr/local/src/cdrengine
# RUN cd /usr/local/src/cdrengine;
# WORKDIR /usr/local/src/cdrengine
# RUN npm install
# EXPOSE 8816
# CMD [ "node", "/usr/local/src/cdrengine/app.js" ]

FROM node:10-alpine
WORKDIR /usr/local/src/cdrengine
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 8819
CMD [ "node", "app.js" ]
