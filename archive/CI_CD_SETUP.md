# CI/CD Setup Guide

## ✅ What's Already Done

1. **CI/CD Workflow Created** - `.github/workflows/ci.yml` is now on your branch
2. **Currently Running** - Check https://github.com/humanjesse/zvdb/actions
3. **Tests Both Branches** - Workflow is on both `claude/` branches

## 🚀 Next Steps: Creating Main Branch

Since you don't have a `main` branch yet, here's how to create one:

### Option 1: Via GitHub UI (Easiest)

1. Go to https://github.com/humanjesse/zvdb
2. Click the branch dropdown (currently shows one of your claude branches)
3. Type `main` in the text box
4. Click "Create branch: main from [current-branch]"
5. Go to **Settings** → **Branches** → Set `main` as the default branch

### Option 2: Via Command Line (You do this)

```bash
# From your local machine (not Claude Code)
git checkout -b main
git push -u origin main

# On GitHub: Settings → Branches → Set main as default
```

## 🔒 Recommended: Set Up Branch Protection

Once `main` exists, protect it:

1. Go to **Settings** → **Branches** → **Add branch protection rule**
2. Branch name pattern: `main`
3. Enable:
   - ✅ Require status checks to pass before merging
   - ✅ Require branches to be up to date before merging
   - ✅ Select: "Test & Build" (your CI workflow)
   - ✅ Require pull request reviews (optional but recommended)

## 📝 Merging Your Feature Branches

After `main` is set up:

### For `claude/add-sql-text-storage-...` branch:

```bash
# Create a PR on GitHub
gh pr create --base main --head claude/add-sql-text-storage-011CV5M1nun9H864JcP5dmFQ

# Or via GitHub UI:
# 1. Go to Pull Requests → New Pull Request
# 2. Base: main
# 3. Compare: claude/add-sql-text-storage-...
# 4. Wait for CI to pass ✅
# 5. Merge!
```

### For `claude/database-exploration-...` branch:

Same process as above, but use the other branch name.

## 🎯 Your CI/CD Workflow

Your workflow automatically:

- ✅ Checks code formatting (`zig fmt`)
- ✅ Builds the library
- ✅ Runs all tests
- ✅ Builds benchmarks
- ✅ Tests on Ubuntu, macOS, and Windows
- ✅ Tests Debug and ReleaseFast builds

**Triggers on:**
- Push to `main` or any `claude/**` branch
- Pull requests to `main`

## 📊 Checking CI Status

- **All runs**: https://github.com/humanjesse/zvdb/actions
- **Specific commit**: Look for the ✅ or ❌ next to commits
- **In PRs**: Status checks appear at the bottom before merge button

## 🐛 If CI Fails

Common issues:
1. **Formatting**: Run `zig fmt src/` locally before committing
2. **Tests failing**: Run `zig build test` locally
3. **Build errors**: Run `zig build` locally

## 💡 Recommendation

**Should you merge branches to main now?**

**My advice: YES, but with this order:**

1. First, create `main` branch (use Option 1 or 2 above)
2. Set up branch protection with CI requirement
3. Create PR from `claude/add-sql-text-storage-...` → `main`
4. Wait for CI ✅ to pass
5. Merge!
6. Repeat for other feature branch

This way, you'll never merge broken code to main. 🎉

---

**Questions?** Check the workflow file at `.github/workflows/ci.yml` or GitHub Actions docs.
