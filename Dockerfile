FROM golang:latest
RUN mkdir /app
COPY build/app /app/app
CMD ["/app/app"]