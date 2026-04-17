pnpm release:check

cd packages/core
npm version minor --no-git-tag-version
cd ../..

cd packages/cli
npm version minor --no-git-tag-version
cd ../..

git add packages/core/package.json packages/cli/package.json
git commit -m "release vX.Y.Z"

pnpm --filter @oorestisime/quarry publish --access public
pnpm --filter @oorestisime/quarry-cli publish --access public

git tag vX.Y.Z
git push
git push --tags