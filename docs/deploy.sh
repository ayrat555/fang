#!/usr/bin/env bash
set -e

BRANCH="gh-pages"

build() {
  echo "Starting building..."

  TIME=$(date +"%Y-%m-%dT%H:%M:%S.00Z")

  printf "+++\ntitle = \"CHANGELOG\"\ndate = $TIME\nupdated = $TIME\ndraft = false\nweight = 410\nsort_by = \"weight\"\ntemplate = \"docs/page.html\"\n\n[extra]\ntoc = true\ntop = false\n+++\n\n" > docs/content/docs/CHANGELOG.md

  cat fang/CHANGELOG.md >> docs/content/docs/CHANGELOG.md

  printf "+++\ntitle = \"README\"\ndate = $TIME\nupdated = $TIME\ndraft = false\nweight = 410\nsort_by = \"weight\"\ntemplate = \"docs/page.html\"\n\n[extra]\ntoc = true\ntop = false\n+++\n\n" > docs/content/docs/README.md

  cat README.md >> docs/content/docs/README.md


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
