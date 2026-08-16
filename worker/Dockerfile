# Mocked batch-inference worker.
#
# Deliberately tiny: python:3.13-slim plus the three libraries the entrypoint
# actually needs. The dependencies are installed at BUILD time rather than at
# pod start -- installing them per pod costs ~15s on every task, on every run,
# and turns PyPI into a hard runtime dependency for a pipeline that otherwise
# only needs to reach MinIO.
#
# Built straight into minikube's docker daemon by scripts/1-build-images.sh, so
# there is no registry and nothing is ever pushed. `imagePullPolicy:
# IfNotPresent` finds it on the node.
FROM python:3.13-slim

# Pinned so a rebuild months from now produces the same worker. numpy is not
# optional: .npy is numpy's own binary format, which is what the pipeline is
# required to emit.
RUN pip install --no-cache-dir \
      "boto3==1.43.0" \
      "numpy==2.4.6" \
      "PyYAML==6.0.3"

# Runs as the Airflow uid/gid rather than root. The scratch volume is shared
# with the Airflow pods (uid 50000, gid 0, setgid) -- matching identities is
# what lets both write to it without making the directory world-writable.
# python:3.13-slim has no passwd entry for 50000, which is fine, but some
# libraries expect a writable HOME, so point it somewhere that always is.
ENV HOME=/tmp \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY --chown=50000:0 entrypoint.py /opt/inference/entrypoint.py
RUN chmod 0755 /opt/inference/entrypoint.py

USER 50000:0

ENTRYPOINT ["python", "/opt/inference/entrypoint.py"]
