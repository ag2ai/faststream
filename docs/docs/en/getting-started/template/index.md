---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 5
---

# Using Cookiecutter FastStream Template

[Cookiecutter FastStream](https://github.com/ag2ai/cookiecutter-faststream){.external-link target="_blank"} is a versatile repository that provides a solid foundation for your Python projects. It comes with a basic application, testing infrastructure, linting scripts, and various development tools to kickstart your development process. Whether you're building a new application from scratch or want to enhance an existing one, this template will save you time and help you maintain high code quality.

## Features

* **Basic Application**: [Cookiecutter FastStream](https://github.com/ag2ai/cookiecutter-faststream){.external-link target="_blank"} includes a basic Python application as a starting point for your project. You can easily replace it with your own code.

* **Testing Framework**: We've set up [`pytest`](https://pytest.org/){.external-link target="_blank"} for running unit tests. Write your tests in the tests directory and use the provided workflow for automated testing.

* **Linting**: Keep your code clean and consistent with linting tools. The repository includes linting scripts and configurations for [`mypy`](https://mypy.readthedocs.io/en/stable/){.external-link target="_blank"}, [`black`](https://github.com/psf/black){.external-link target="_blank"}, [`ruff`](https://github.com/astral-sh/ruff){.external-link target="_blank"} and [`bandit`](https://bandit.readthedocs.io/en/latest/){.external-link target="_blank"}

* **Docker Support**: The included Dockerfile allows you to containerize your [`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} application. Build and run your application in a containerized environment with ease.

* **Dependency Management**: All application requirements and development dependencies are specified in the `pyproject.toml` file. This includes not only your project's dependencies but also configurations for various tools like [`pytest`](https://pytest.org/){.external-link target="_blank"}, [`mypy`](https://mypy.readthedocs.io/en/stable/){.external-link target="_blank"}, [`black`](https://github.com/psf/black){.external-link target="_blank"}, [`ruff`](https://github.com/astral-sh/ruff){.external-link target="_blank"}, and [`bandit`](https://bandit.readthedocs.io/en/latest/){.external-link target="_blank"}.

* **Continuous Integration (CI)**: [Cookiecutter FastStream](https://github.com/ag2ai/cookiecutter-faststream){.external-link target="_blank"} comes with three [GitHub Actions](https://github.com/features/actions){.external-link target="_blank"} workflows under the `.github/workflows` directory:

  1. **Static Analysis and Testing**: This workflow consists of two jobs. The first job runs static analysis tools ([`mypy`](https://mypy.readthedocs.io/en/stable/){.external-link target="_blank"} and [`bandit`](https://bandit.readthedocs.io/en/latest/){.external-link target="_blank"}) to check your code for potential issues. If successful, the second job runs [`pytest`](https://pytest.org/){.external-link target="_blank"} to execute your test suite.

  2. **Docker Build and Push**: This workflow automates the process of building a [`Docker`](https://www.docker.com/){.external-link target="_blank"} image for your [`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} application and pushing it to the [GitHub Container Registry](https://ghcr.io){.external-link target="_blank"}.

  3. **AsyncAPI Documentation**: The third workflow builds [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation for your [`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} application and deploys it to [GitHub Pages](https://pages.github.com/){.external-link target="_blank"}. This is useful for documenting your API and making it accessible to others.

## Getting Started

To set up your development environment, follow these steps:

1. Install the [`cookiecutter`](https://github.com/cookiecutter/cookiecutter){.external-link target="_blank"} package using the following command:
   ```bash
   pip install cookiecutter
   ```

2. Run the provided [`cookiecutter`](https://github.com/cookiecutter/cookiecutter){.external-link target="_blank"} command and fill out the relevant details to generate a new FastStream project:
   ```bash
   cookiecutter https://github.com/ag2ai/cookiecutter-faststream.git
   ```
   The following screenshot illustrates the process of creating a new FastStream app with Kafka using the above command:![using-cookiecutter-faststream](https://github.com/ag2ai/faststream/assets/7011056/d24ee8ec-3b39-4b85-9bba-a6be823a5fed)

3. Change the working directory to the newly created directory:
   ```bash
   cd <directory-name>
   ```
   > **_NOTE:_** Replace `<directory-name>` with the name of your directory.

4. Install all development requirements using pip:
   ```bash
   pip install -e ".[dev]"
   ```

5. Create a new repository for our FastStream app on GitHub.![creating-new-github-repo](https://github.com/ag2ai/faststream/assets/7011056/7076b925-2090-4bbb-b9da-0df4783fb5a3)

6. Add all the files, commit and push using the following commands:
   ```bash
   git init
   git add .
   git commit -m "first commit"
   git branch -M main
   git remote add origin git@github.com:<username>/<repo-name>.git
   git push -u origin main
   ```
   > **_NOTE:_** Replace `<username>` with your GitHub username and `<repo-name>` with the name of your repository in the above commands.

## Development

The application code is located in the `app/` directory. You can add new features or fix bugs in this directory. However, remember that code changes must be accompanied by corresponding updates to the tests located in the `tests/` directory.

## Running Tests

Once you have updated tests, you can execute the tests using [`pytest`](https://pytest.org/){.external-link target="_blank"}:

```bash
pytest
```

## Running FastStream Application Locally

To run the [`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} application locally, follow these steps:

1. Start the Kafka Docker container locally using the provided script:
   ```bash
   ./scripts/start_kafka_broker_locally.sh
   ```

2. Start the [`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} application with the following command:
   ```bash
   faststream run <directory-name>.application:app --workers 1
   ```
   > **_NOTE:_** Replace `<directory-name>` with the directory that is automatically generated from the project slug name and contains the `app.py` file.

3. You can now send messages to the Kafka topic and can test the application. Optionally, if you want to view messages in a topic, you can subscribe to it using the provided script:
   ```bash
   ./scripts/subscribe_to_kafka_broker_locally.sh <topic_name>
   ```

4. To stop the [`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} application, press `Ctrl+C`.

5. Finally, stop the Kafka Docker container by running the script:
   ```bash
   ./scripts/stop_kafka_broker_locally.sh
   ```

## Building and Testing Docker Image Locally

If you'd like to build and test the [`Docker`](https://www.docker.com/){.external-link target="_blank"} image locally, follow these steps:

1. Run the provided script to build the [`Docker`](https://www.docker.com/){.external-link target="_blank"} image locally. Use the following command:
   ```bash
   ./scripts/build_docker.sh <username> <repo-name>
   ```
   This script will build the [`Docker`](https://www.docker.com/){.external-link target="_blank"} image locally with the same name as the one built in `CI`.

2. Before starting the [`Docker`](https://www.docker.com/){.external-link target="_blank"} container, ensure that a Kafka [`Docker`](https://www.docker.com/){.external-link target="_blank"} container is running locally. You can start it using the provided script:
   ```bash
   ./scripts/start_kafka_broker_locally.sh
   ```

3. Once Kafka is up and running, you can start the local [`Docker`](https://www.docker.com/){.external-link target="_blank"} container using the following command:
   ```bash
   docker run --rm --name faststream-app --net=host ghcr.io/<username>/<repo-name>:latest
   ```
   `--rm`: This flag removes the container once it stops running, ensuring that it doesn't clutter your system with unused containers.
   `--name faststream-app`: Assigns a name to the running container, in this case, "faststream-app".
   `--net=host`: This flag allows the [`Docker`](https://www.docker.com/){.external-link target="_blank"} container to share the host's network namespace.

4. To stop the local [`Docker`](https://www.docker.com/){.external-link target="_blank"} container, simply press `Ctrl+C` in your terminal.

5. Finally, stop the Kafka [`Docker`](https://www.docker.com/){.external-link target="_blank"} container by running the provided script:
   ```bash
   ./scripts/stop_kafka_broker_locally.sh
   ```

> **_NOTE:_** Replace `<username>` with your GitHub username and `<repo-name>` with the name of your repository in the above commands.

## Code Linting

After making changes to the code, it's essential to ensure it adheres to coding standards. We provide a script to help you with code formatting and linting. Run the following script to automatically fix linting issues:

```bash
./scripts/lint.sh
```

## Static Analysis

Static analysis tools [`mypy`](https://mypy.readthedocs.io/en/stable/){.external-link target="_blank"} and [`bandit`](https://bandit.readthedocs.io/en/latest/){.external-link target="_blank"} can help identify potential issues in your code. To run static analysis, use the following script:

```bash
./scripts/static-analysis.sh
```

If there are any static analysis errors, resolve them in your code and rerun the script until it passes successfully.

## Viewing AsyncAPI Documentation

[`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} framework supports [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation. To ensure that your changes are reflected in the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation, follow these steps:

1. Run the following command to view the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation:
   ```bash
   faststream docs serve <directory-name>.application:app
   ```
   This command builds the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} specification file, generates [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation based on the specification, and serves it at `localhost:8000`.
   > **_NOTE:_** Replace `<directory-name>` with the directory that is automatically generated from the project slug name and contains the `app.py` file.

2. Open your web browser and navigate to <http://localhost:8000> to view the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation reflecting your changes.

3. To stop the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation server, press `Ctrl+C`.

## Contributing

Once you have successfully completed all the above steps, you are ready to contribute your changes:

1. Add and commit your changes:
   ```bash
   git add .
   git commit -m "Your commit message"
   ```

2. Push your changes to GitHub:
   ```bash
   git push origin your-branch
   ```

3. Create a merge request on GitHub.

## Continuous Integration (CI)

This repository is equipped with GitHub Actions that automate static analysis and pytest in the CI pipeline. Even if you forget to perform any of the required steps, CI will catch any issues before merging your changes.

This repository has three workflows, each triggered when code is pushed:

1. **Tests Workflow**: This workflow is named "Tests" and consists of two jobs. The first job runs static analysis tools [`mypy`](https://mypy.readthedocs.io/en/stable/){.external-link target="_blank"} and [`bandit`](https://bandit.readthedocs.io/en/latest/){.external-link target="_blank"} to identify potential issues in the codebase. The second job runs tests using [`pytest`](https://pytest.org/){.external-link target="_blank"} to ensure the functionality of the application. Both jobs run simultaneously to expedite the `CI` process.

2. **Build Docker Image Workflow**: This workflow is named "Build Docker Image" and has one job. In this job, a [`Docker`](https://www.docker.com/){.external-link target="_blank"} image is built based on the provided Dockerfile. The built image is then pushed to the [**GitHub Container Registry**](https://ghcr.io){.external-link target="_blank"}, making it available for deployment or other purposes.

3. **Deploy FastStream AsyncAPI Docs Workflow**: The final workflow is named "Deploy FastStream AsyncAPI Docs" and also consists of a single job. In this job, the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation is built from the specification, and the resulting documentation is deployed to [**GitHub Pages**](https://pages.github.com/){.external-link target="_blank"}. This allows for easy access and sharing of the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation with the project's stakeholders.

## Viewing AsyncAPI Documentation Hosted at GitHub Pages

After the **Deploy FastStream AsyncAPI Docs** workflow in `CI` has been successfully completed, the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation is automatically deployed to [**GitHub Pages**](https://pages.github.com/){.external-link target="_blank"}. This provides a convenient way to access and share the documentation with project stakeholders.

To view the deployed [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation, open your web browser and navigate to the following URL:

```txt
https://<username>.github.io/<repo-name>/
```

> **_NOTE:_** Replace `<username>` with your GitHub username and `<repo-name>` with the name of your repository.

You will be directed to the [**GitHub Pages**](https://pages.github.com/){.external-link target="_blank"} site where your [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation is hosted. This hosted documentation allows you to easily share your [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} specifications with others and provides a centralized location for reviewing the [`AsyncAPI`](https://www.asyncapi.com/){.external-link target="_blank"} documentation.

## Deploying Docker Container

Once the **Build Docker Image** workflow in `CI` has successfully completed, the built [`Docker`](https://www.docker.com/){.external-link target="_blank"} image is pushed to the [**GitHub Container Registry**](https://ghcr.io){.external-link target="_blank"}. You can then deploy this image on your server by following these steps:

1. Pull the [`Docker`](https://www.docker.com/){.external-link target="_blank"} image from the [**GitHub Container Registry**](https://ghcr.io){.external-link target="_blank"} to your server using the following command:
   ```bash
   docker pull ghcr.io/<username>/<repo-name>:latest
   ```
   > **_NOTE:_** Replace `<username>` with your GitHub username and `<repo-name>` with the name of your repository.

2. After successfully pulling the image, start the [`Docker`](https://www.docker.com/){.external-link target="_blank"} container using the following command:
   ```bash
   docker run --rm --name faststream-app --env-file /path/to/env-file ghcr.io/<username>/<repo-name>:latest
   ```
   `--rm`: This flag removes the container once it stops running, ensuring that it doesn't clutter your system with unused containers.
   `--name faststream-app`: Assigns a name to the running container, in this case, "faststream-app".
   `--env-file /path/to/env-file`: Specifies the path to an environment file (commonly a `.env` file) that contains environment variables required by your [`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} application. Storing secrets and configuration in an environment file is a secure and best practice for handling sensitive information such as Kafka host, port, and authentication details.

By following these steps, you can easily deploy your [`FastStream`](https://github.com/ag2ai/faststream){.external-link target="_blank"} application as a [`Docker`](https://www.docker.com/){.external-link target="_blank"} container on your server. Remember to customize the `env-file` and other environment variables as needed to suit your specific application requirements.
