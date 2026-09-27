# Builds on top of the existing custom image rather than recreating it from
# scratch -- whatever's already customized in there (Python, code-server
# itself, etc.) carries through untouched. This layer adds the things that
# had to be manually re-bootstrapped on every fresh Akash volume: uv, Node.js
# (confirmed NOT present in the base image -- `which node` returns nothing),
# tclk/'s runtime dependencies, and the application code itself.
#
# Pinned to the ORIGINAL pre-seed image by digest, not :latest -- the first
# build of this file was pushed back over :latest, so FROM :latest would stack
# these layers on top of their own previous output on every rebuild.
# Published builds: :latest2 (uv + app code), :latest3 (+ Node, tclk deps).
FROM justuncase/technocore-archive-codeserver@sha256:3e574225ca2707dbbd747378d2b0c29d8392c5c0074ea972d47af9a447a9c077

# uv is staged OUTSIDE /config on purpose. /config is the Akash
# persistent-volume mount point -- anything baked into the image AT that
# path is invisible at runtime because the (empty, on a fresh volume) host
# volume shadows it the moment it's mounted. Only a script that runs AFTER
# the volume is mounted can actually seed it (see seed-defaults.sh).
RUN curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/opt/uv-seed sh

# Node.js -- needed by tclk_audit.mjs (the tclk/1 deal-audit endpoint).
# Installed to /usr/local, which is NOT under /config, so it's available
# immediately at every boot with no seeding step required.
ARG NODE_VERSION=20.19.0
RUN curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.gz" -o /tmp/node.tar.gz \
    && tar -xzf /tmp/node.tar.gz -C /usr/local --strip-components=1 \
    && rm /tmp/node.tar.gz \
    && node --version && npm --version

COPY . /opt/workspace-seed

# tclk/'s runtime deps (@noble/curves, @noble/hashes, @scure/base) aren't
# committed to git (tclk/node_modules/ is gitignored) -- installed here at
# build time so a fresh Akash volume never needs outbound npm access, and
# tclk_audit.mjs works the first time archive_api starts rather than only
# after a manual `npm install` on the box.
RUN cd /opt/workspace-seed/tclk && npm install --omit=dev

# LSIO convention: anything dropped in /etc/cont-init.d/ runs once per
# container start, before services, as root. Idempotent by design (see
# seed-defaults.sh) so it never clobbers real data already on the volume --
# safe to run on every boot, not just the first.
COPY seed-defaults.sh /etc/cont-init.d/90-seed-defaults
RUN chmod +x /etc/cont-init.d/90-seed-defaults
