FROM httpd

# Minimum compatible git commit, please only use later commits.
ARG IVA_COMMIT_SHA="dce421b6933d540d9c511c72e63c0e85dff00667"

# Install dependencies
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y curl gnupg2 && \
    curl -sL https://deb.nodesource.com/setup_8.x | bash && \
    apt-get install -y git nodejs npm

RUN echo '{ "allow_root": true }' > /root/.bowerrc && \
    npm install -g grunt-cli

# Download and extract the IVA configuration into our local configuration
# NOTE: We do not clone directly as we cannot clone into this existing dir
RUN cd /usr/local/apache2/htdocs/ && \
    git init && \
    git remote add origin "https://github.com/opencb/iva.git" && \
    git fetch --all --prune && \
    git reset "$IVA_COMMIT_SHA" --hard && \
    git submodule update --init && \
    cd lib/jsorolla && \
    git checkout develop && \
    npm install && \
    cd ../.. && \
    npm install

EXPOSE 80

VOLUME usr/local/apache2/htdocs/conf
