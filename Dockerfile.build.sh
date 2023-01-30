VERSION=`cat main_57.version`
docker build --platform linux/amd64 -t shinhwagk/pl2es:${VERSION} .
docker push shinhwagk/pl2es:${VERSION}