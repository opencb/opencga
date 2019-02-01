FROM httpd

# Install dependencies
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y curl gnupg2 && \
    curl -sL https://deb.nodesource.com/setup_8.x | bash && \
    apt-get install -y git nodejs npm

RUN echo '{ "allow_root": true }' > /root/.bowerrc && \
    npm install -g grunt-cli

# Download and extract the IVA configuration into our local configuration
RUN git clone -b "master" "https://github.com/opencb/iva.git" && \
    cd iva && \
    git submodule update --init && \
    cd lib/jsorolla && \
    git checkout develop && \
    npm install && \
    cd ../.. && \
    npm install --unsafe-perm && \
    npm run build && \
    cp -r build/* /usr/local/apache2/htdocs

EXPOSE 80

VOLUME /usr/local/apache2/htdocs/conf
