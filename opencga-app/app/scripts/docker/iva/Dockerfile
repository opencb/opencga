FROM httpd

# Minimum compatible git commit, please only use later commits.
ARG IVA_COMMIT_SHA="dce421b6933d540d9c511c72e63c0e85dff00667"

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
    wget \
    git

# Download and extract the IVA configuration into our local configuration
RUN cd /usr/local/apache2/htdocs/ && \
    git clone "https://github.com/opencb/iva.git" && \
    cd iva && \
    git reset "$IVA_COMMIT_SHA" --hard

EXPOSE 80

VOLUME usr/local/apache2/htdocs/conf
