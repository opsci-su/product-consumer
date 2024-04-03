FROM node:18-alpine
COPY . .
RUN yarn
CMD node main.js
