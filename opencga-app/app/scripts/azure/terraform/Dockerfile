FROM microsoft/azure-cli:2.0.57

RUN wget https://releases.hashicorp.com/terraform/0.11.11/terraform_0.11.11_linux_386.zip && \
    mkdir -p terraform && \
    unzip terraform_0.11.11_linux_386.zip -d terraform && \
    mv terraform/terraform /usr/local/bin

COPY . .

RUN chmod +x ./entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]