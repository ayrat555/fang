#!/usr/bin/env bash
set -e

BRANCH="gh-pages"

build() {
  echo "Starting building..."

  cp -R docs ../docs_backup
  rm -r *
  cp -R ../docs_backup ./docs
  cd docs

  sudo snap install --edge zola
  zola build
  mv public /tmp/public
  cd ..
}

deploy() {
  echo "Starting deploying..."
  git config --global url."https://".insteadOf git://
  git config --global url."https://github.com/".insteadOf git@github.com:

  git checkout ${BRANCH}
  cp -vr /tmp/public/* .
  git config user.name "GitHub Actions"
  git config user.email "github-actions-bot@users.noreply.github.com"

  rm -r docs/themes
  git add .
  git commit -m "Deploy new version docs"
  git push --force "https://${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git" ${BRANCH}

  echo "Deploy complete"
}
