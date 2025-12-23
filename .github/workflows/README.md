# GitHub Actions Workflows

This directory contains the CI/CD workflows for the DSBulk project.

## Workflows Overview

### 1. Build and Test (`build.yml`)
**Triggers**: Push to main/master, Pull Requests, Manual dispatch

**Purpose**: Primary CI workflow for fast feedback on code changes

**What it does**:
- Builds the project with JDK 8 and 11
- Runs unit tests
- Publishes test results
- Caches Maven dependencies for faster builds

**Duration**: ~5-10 minutes

### 2. Integration Tests (`integration-tests.yml`)
**Triggers**: Pull Requests, Manual dispatch

**Purpose**: Comprehensive integration testing with Docker-based Cassandra/DSE

**What it does**:
- Tests against Cassandra 3.11, 4.0, 4.1, 5.0
- Tests against DSE 5.1.48, 6.8.61, 6.9.17
- Uses Docker service containers (no CCM required)
- Supports medium and long test profiles
- Publishes detailed test results

**Duration**: ~15-30 minutes per matrix job

**Manual Trigger Options**:
- `test_profile`: Choose between `medium` (default) or `long` test profiles

### 3. Release (`release.yml`)
**Triggers**: Git tags (v*), Manual dispatch

**Purpose**: Build and publish release artifacts

**What it does**:
- Builds with release profile
- Runs full test suite (medium + long)
- Generates distribution artifacts:
  - `dsbulk-{version}.tar.gz` (Linux/Mac)
  - `dsbulk-{version}.zip` (Windows)
  - `dsbulk-{version}.jar` (Standalone)
- Creates SHA256 checksums
- Uploads to GitHub Releases
- Generates release notes

**Duration**: ~30-60 minutes

**Manual Trigger Options**:
- `version`: Specify version number (e.g., "1.11.1")

**Creating a Release**:
```bash
# Tag and push
git tag -a v1.11.1 -m "Release 1.11.1"
git push origin v1.11.1

# Or trigger manually via GitHub UI
```

### 4. Nightly Build (`nightly.yml`)
**Triggers**: Scheduled (Mon-Fri at 6 AM UTC), Manual dispatch

**Purpose**: Comprehensive nightly testing across full matrix

**What it does**:
- Runs full matrix tests (all Cassandra + DSE versions)
- Executes long-running test profiles
- Generates distribution artifacts
- Creates build summary
- Optional Slack notifications

**Duration**: ~1-2 hours

**Manual Trigger Options**:
- `generate_artifacts`: Enable/disable artifact generation (default: true)

### 5. Code Quality (`code-quality.yml`)
**Triggers**: Push to main/master, Pull Requests, Manual dispatch

**Purpose**: Code quality checks and coverage reporting

**What it does**:
- Generates JaCoCo code coverage reports
- Uploads coverage to Codecov
- Checks code formatting (fmt-maven-plugin)
- Validates license headers
- Runs SpotBugs analysis
- Checks for dependency vulnerabilities
- Posts coverage comments on PRs

**Duration**: ~10-15 minutes

## Workflow Dependencies

```
build.yml (fast feedback)
    ↓
integration-tests.yml (comprehensive testing)
    ↓
code-quality.yml (quality checks)
    ↓
release.yml (on tags) or nightly.yml (scheduled)
```

## Docker Images Used

### Cassandra (Public - Docker Hub)
- `cassandra:3.11` - Latest 3.11.x
- `cassandra:4.0` - Latest 4.0.x
- `cassandra:4.1` - Latest 4.1.x
- `cassandra:5.0` - Latest 5.0.x

### DataStax Enterprise (Public - Docker Hub)
- `datastax/dse-server:5.1.35`
- `datastax/dse-server:6.8.49`
- `datastax/dse-server:6.9.0`

**Note**: All images are publicly available - no credentials required!

## Secrets Configuration

### Required Secrets
None! All dependencies are available via Maven Central, and Docker images are public.

### Optional Secrets
- `SLACK_WEBHOOK_URL` - For Slack notifications (nightly builds)
- `GPG_PRIVATE_KEY` - For signing releases (if needed)
- `GPG_PASSPHRASE` - For signing releases (if needed)
- `CODECOV_TOKEN` - For Codecov integration (optional, works without it for public repos)

## Manual Workflow Triggers

All workflows support manual triggering via GitHub UI:

1. Go to **Actions** tab
2. Select the workflow
3. Click **Run workflow**
4. Choose branch and options
5. Click **Run workflow**

## Caching Strategy

Maven dependencies are cached using `actions/cache@v4`:
- **Cache key**: Based on `pom.xml` files
- **Cache path**: `~/.m2/repository`
- **Restore keys**: Fallback to OS-specific Maven cache

This significantly speeds up builds (2-3x faster after first run).

## Test Result Reporting

All workflows use `EnricoMi/publish-unit-test-result-action@v2` to:
- Parse JUnit XML reports
- Create check runs with test results
- Show pass/fail statistics
- Highlight flaky tests
- Comment on PRs with results

## Artifact Retention

| Artifact Type | Retention Period |
|---------------|------------------|
| Test results | 7-14 days |
| Coverage reports | 30 days |
| Nightly builds | 30 days |
| Release artifacts | 90 days |
| GitHub Releases | Permanent |

## Troubleshooting

### Build Failures

**Maven dependency issues**:
```bash
# Clear cache and retry
rm -rf ~/.m2/repository
```

**Docker service not ready**:
- Workflows include health checks and wait loops
- Increase timeout if needed (currently 30 attempts × 10s = 5 minutes)

**Test failures**:
- Check test reports in artifacts
- Review logs for specific failure reasons
- CCM-dependent tests may need adaptation

### Performance Issues

**Slow builds**:
- Check if Maven cache is working
- Consider reducing test matrix for PRs
- Use `workflow_dispatch` with specific options

**Timeout issues**:
- Default timeout: 4 hours (matching Jenkins)
- Adjust in workflow file if needed

## Migration from Jenkins/Travis

### Key Differences

| Aspect | Jenkins/Travis | GitHub Actions |
|--------|---------------|----------------|
| Infrastructure | CCM | Docker containers |
| Cassandra versions | 2.1-3.11 | 3.11, 4.0, 4.1, 5.0 |
| DSE versions | 4.7-6.8 | 5.1, 6.8, 6.9 |
| Artifacts | Jenkins storage | GitHub Releases |
| Secrets | Artifactory creds | None required |

### Gradual Migration

1. **Phase 1**: Run GitHub Actions in parallel with Jenkins
2. **Phase 2**: Validate results match
3. **Phase 3**: Switch primary CI to GitHub Actions
4. **Phase 4**: Deprecate Jenkins/Travis

## Contributing

When adding new workflows:
1. Follow existing naming conventions
2. Add comprehensive comments
3. Include manual trigger options
4. Update this README
5. Test thoroughly before merging

## Support

For issues or questions:
- Check workflow logs in Actions tab
- Review [ci-modernization.md](../../ci-modernization.md) for details
- Open an issue with workflow run link