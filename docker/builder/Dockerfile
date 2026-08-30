FROM --platform=${BUILDPLATFORM} icr.io/db2_community/db2 AS builder

FROM rockylinux:9
ARG PGVERSION
ENV PGVERSION=${PGVERSION}

# The FDW only links against include/lib64 at build time, but the postgres
# image derived from this one runs against DB2 with the CLI, which needs the
# full client runtime (cfg, conv, msg, properties, security, instance tools).
COPY --from=builder /opt/ibm/db2/V12.1/include /opt/ibm/db2/V12.1/include
COPY --from=builder /opt/ibm/db2/V12.1/lib64 /opt/ibm/db2/V12.1/lib64
COPY --from=builder /opt/ibm/db2/V12.1/bin /opt/ibm/db2/V12.1/bin
COPY --from=builder /opt/ibm/db2/V12.1/adm /opt/ibm/db2/V12.1/adm
COPY --from=builder /opt/ibm/db2/V12.1/bnd /opt/ibm/db2/V12.1/bnd
COPY --from=builder /opt/ibm/db2/V12.1/cfg /opt/ibm/db2/V12.1/cfg
COPY --from=builder /opt/ibm/db2/V12.1/conv /opt/ibm/db2/V12.1/conv
COPY --from=builder /opt/ibm/db2/V12.1/function /opt/ibm/db2/V12.1/function
COPY --from=builder /opt/ibm/db2/V12.1/gskit /opt/ibm/db2/V12.1/gskit
COPY --from=builder /opt/ibm/db2/V12.1/install /opt/ibm/db2/V12.1/install
COPY --from=builder /opt/ibm/db2/V12.1/instance /opt/ibm/db2/V12.1/instance
COPY --from=builder /opt/ibm/db2/V12.1/license /opt/ibm/db2/V12.1/license
COPY --from=builder /opt/ibm/db2/V12.1/map /opt/ibm/db2/V12.1/map
COPY --from=builder /opt/ibm/db2/V12.1/msg /opt/ibm/db2/V12.1/msg
COPY --from=builder /opt/ibm/db2/V12.1/properties /opt/ibm/db2/V12.1/properties
COPY --from=builder /opt/ibm/db2/V12.1/security32 /opt/ibm/db2/V12.1/security32
COPY --from=builder /opt/ibm/db2/V12.1/security64 /opt/ibm/db2/V12.1/security64
COPY --from=builder /opt/ibm/db2/V12.1/tools /opt/ibm/db2/V12.1/tools
COPY scripts /scripts

RUN   dnf install -y epel-release && \
      dnf install 'dnf-command(config-manager)' && \
      dnf config-manager --enable crb && \
			# See https://forums.rockylinux.org/t/status-code-404-on-mirrors-upon-dnf-update/3204/6
			find /etc/yum.repos.d -name "Rocky-*.repo" -exec sed -i 's/^mirrorlist=/#&/' {} \; -exec sed -i '/^#baseurl=/s/^#//' {} \; && \
      dnf update -y && \
      dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
      dnf -qy module disable postgresql && \
      dnf install -y make gcc clang llvm llvm-devel postgresql${PGVERSION}-server postgresql${PGVERSION}-devel git rpm-build rpm-sign
