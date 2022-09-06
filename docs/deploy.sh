#!/usr/bin/env bash
set -e

BRANCH="gh-pages"

current_time() {
  
  TIME=$(date -u --rfc-3339=seconds)

  DATE=$(echo $TIME | cut -d' ' -f1)
  HOUR=$(echo $TIME | cut -d' ' -f2)
  
  VALID_TIME=${DATE}T${HOUR}
  
  echo $VALID_TIME

}

build() {
  echo "Starting building..."

  TIME=$(current_time)

  printf "+++\ntitle = \"CHANGELOG\"\ndescription = \"Fang CHANGELOG\"\ndate = $TIME\nupdated = $TIME\ndraft = false\nweight = 410\nsort_by = \"weight\"\ntemplate = \"docs/page.html\"\n\n[extra]\nlead = \"Fang Changelog\"\ntoc = true\ntop = false\n+++\n\n" > docs/content/docs/CHANGELOG.md

  cat CHANGELOG.md >> docs/content/docs/CHANGELOG.md

  printf "+++\ntitle = \"README\"\ndescription = \"Fang README\"\ndate = $TIME\nupdated = $TIME\ndraft = false\nweight = 410\nsort_by = \"weight\"\ntemplate = \"docs/page.html\"\n\n[extra]\nlead = \"Fang README\"\ntoc = true\ntop = false\n+++\n\n" > docs/content/docs/README.md

  cat README.md >> docs/content/docs/README.md

  
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
