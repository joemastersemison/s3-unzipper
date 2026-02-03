# Git Hooks

## Emergency Bypass
In case of urgent commits where hooks are failing:
```bash
git commit --no-verify -m "emergency: your message"
```

## Manual Hook Execution
To run the pre-commit hooks manually:
```bash
npx lint-staged
```