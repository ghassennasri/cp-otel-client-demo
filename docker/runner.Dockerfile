# Ansible runner with the official cp-ansible collection (pinned tag) and the Docker CLI.
# Used as: docker compose run --rm ansible <playbook>
FROM docker:27.5.1-cli AS dockercli
FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends git curl ca-certificates openssh-client && rm -rf /var/lib/apt/lists/*
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
RUN pip install --no-cache-dir ansible==11.6.0 requests cryptography
ARG CP_ANSIBLE_VERSION=8.3.2
RUN mkdir -p /opt/collections/ansible_collections/confluent && git clone --depth 1 --branch v${CP_ANSIBLE_VERSION} https://github.com/confluentinc/cp-ansible.git /opt/collections/ansible_collections/confluent/platform
ENV ANSIBLE_COLLECTIONS_PATH=/opt/collections:/usr/local/lib/python3.12/site-packages
WORKDIR /demo
ENTRYPOINT ["ansible-playbook", "-i", "ansible/inventory.yml"]
