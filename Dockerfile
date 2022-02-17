FROM openjdk:11

RUN mkdir /streamliner && mkdir /assets

COPY build/install/streamliner/bin /streamliner/bin
COPY build/install/streamliner/lib /streamliner/lib

COPY build/install/streamliner/conf /assets/conf
COPY build/install/streamliner/templates /assets/templates

WORKDIR /streamliner/bin

ENTRYPOINT ["./streamliner"]
