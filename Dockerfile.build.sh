# scrape
VERSION=`cat main_57.version`
docker build --platform linux/amd64 -t shinhwagk/pl2es:scrape-${VERSION} -f Dockerfile.scrape .
docker push shinhwagk/pl2es:scrape-${VERSION}
echo shinhwagk/pl2es:scrape-${VERSION}

# process
VERSION=`cat main_57.version`
docker build --platform linux/amd64 -t shinhwagk/pl2es:process-${VERSION} -f Dockerfile.process .
docker push shinhwagk/pl2es:process-${VERSION}
echo shinhwagk/pl2es:process-${VERSION}