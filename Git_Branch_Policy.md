# 🧩 Git Branch Policy

## Overview
This document defines the branching strategy for managing development, release, and maintenance cycles.  
The goal is to enable **continuous development**, **controlled releases**, and **quick issue resolution** while maintaining stability in production.

---

## Branch Types

| Branch Type | Purpose | Naming Convention | Created From |
|--------------|----------|------------------|---------------|
| **`DEV`** | Main integration branch for all completed release cycles | `DEV` | — |
| **Release Branch** | For active release development cycles | `_R#` (e.g., `_R1`, `_R2`) | `DEV` |
| **Hotfix Branch** | For urgent production fixes | `hotfix/<issue-id>` | Latest deployed release branch |
| **Emergency Branch** | For critical system-down or blocking fixes requiring immediate patching | `emergency/<issue-id>` | Production-deployed tag or branch |

---

## Branching Workflow

### 1. New Development
- All new development work happens in **release branches** named `_R#` (e.g., `_R1`, `_R2`, etc.).  
- Developers must **not commit directly to `DEV`**.

### 2. Creating a Release Branch
- Each release branch is created from the latest `DEV` branch:
  ```bash
  git checkout DEV
  git pull origin DEV
  git checkout -b _R1
  ```

### 3. Completing a Release Cycle
- Once `_R1` is **development complete**, merge it back into `DEV`:
  ```bash
  git checkout DEV
  git merge _R1
  git push origin DEV
  ```
- Tag the release for traceability:
  ```bash
  git tag -a v1.0 -m "Release 1.0 - Completed"
  git push origin v1.0
  ```

---

## Overlapping Development and Deployment

### 4. Next Release Preparation
- Even if `_R1` deployment is still pending, create `_R2` from `DEV` to start the next release cycle:
  ```bash
  git checkout DEV
  git checkout -b _R2
  ```

### 5. Post-Deployment Fixes
- If issues arise after `_R1` deployment, apply fixes directly to `_R1`.  
- Once resolved, manually apply (cherry-pick) those commits to `_R2` to keep the codebase consistent:
  ```bash
  git checkout _R2
  git cherry-pick <commit-hash>
  ```

---

## Hotfix and Emergency Branches

### 6. Hotfix Branches
- Used for urgent but **non-critical** production bugs discovered after a release.  
- Created from the **latest deployed release branch** or **production tag**.
  ```bash
  git checkout -b hotfix/<issue-id> v1.0
  ```
- After testing and validation:
  - Merge the hotfix into both the production branch (if exists) and `DEV`.
  - Cherry-pick the change into the **current active release branch** (e.g., `_R2`).

### 7. Emergency Branches
- Used for **critical issues** that require immediate attention (e.g., outages, security vulnerabilities).  
- These branches are created from the **current production tag**:
  ```bash
  git checkout -b emergency/<issue-id> v1.0
  ```
- Once the emergency fix is deployed:
  - Merge it into both `DEV` and the active release branch (e.g., `_R2`).
  - Document the incident and resolution in the release notes.

---

## Release Planning and Sprint Cycle

### 8. Release Planning
- Each release branch corresponds to a **planned release milestone** (e.g., monthly or quarterly).  
- Features are scoped based on **business priorities** and **team capacity**.
- A **release readiness review** is conducted before merging the branch into `DEV`.

### 9. Sprint Cycle (Placeholder)
- Each sprint duration: **_TBD (e.g., 2 or 3 weeks)_**  
- Number of sprints per release: **_TBD (e.g., 3 sprints per release)_**  
- Deliverables are tracked via project management tools (e.g., Jira, Azure DevOps).

---

## Notes and Best Practices

- Always use **pull requests (PRs)** for merges — no direct commits to shared branches.  
- Ensure all branches are **rebased regularly** with `DEV` to reduce merge conflicts.  
- Tag each production deployment for traceability (`v1.0`, `v1.1`, etc.).  
- All merges into `DEV` or `main` must pass **CI/CD validation and code review**.  
- Hotfix and emergency changes must always be **retrofitted** into ongoing release branches.  

---

## Example Branch Flow Diagram

```text
DEV ──┬─────┬──────────────┬────────────▶
       │     │              │
      _R1   _R2            _R3
       │     │
     (fix) (new features)
       │
   hotfix/issue-101
       │
   emergency/critical-fix
```
