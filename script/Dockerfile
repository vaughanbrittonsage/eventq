FROM ruby:2.4-alpine3.8

RUN apk add --update ca-certificates bash && update-ca-certificates && rm -rf /var/cache/apk/*

RUN set -ex \
	&& apk add --no-cache --virtual .gem-builddeps \
	ruby-dev build-base libressl-dev \
	&& gem install -N json --version "2.1.0" \
	&& gem install -N oj --version "3.6.10" \
	&& gem install -N openssl --version "2.1.1" \
	&& gem install -N byebug --version "10.0.2" \
	&& gem install -N pry-byebug \
	&& apk del .gem-builddeps
