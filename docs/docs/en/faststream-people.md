---
hide:
  - navigation
search:
  exclude: true
---

# FastStream People

**FastStream** has an amazing community that welcomes people from all backgrounds.

## Team

The following are the top contributors to **FastStream**.

These dedicated individuals not only focus on developing and enhancing the **FastStream** framework but also take care of other crucial aspects, such as documentation, workflows, and overall project management. Their continuous efforts help ensure that **FastStream** remains robust, well-maintained, and up-to-date for its users.

<div class="user-list user-list-center">
{% for user in people.people %}
    {% if 'team' in user.include %}
        <div class="user">
            <a href="{{ user.github }}" target="_blank">
                <div class="avatar-wrapper">
                    <img src="{{ user.avatar }}"/>
                </div>
                <div class="title">
                    {% if user.name %}{{user.name}}<br/>{% endif %}
                    @{{user.username}}
                </div>
            </a>
        </div>
    {% endif %}
{% endfor %}
</div>

## Experts by Section

**FastStream** is supported by different experts who specialize in various sections of the framework. For example, Kafka has a team of experts dedicated to its integration and optimization, while RabbitMQ and NATS each have their own specialized teams. This ensures that each messaging protocol within **FastStream** is thoroughly maintained and developed with the highest level of expertise.

### Kafka

<div class="user-list user-list-center">
{% for user in people.people %}
    {% if 'kafka' in user.include %}
        <div class="user">
            <a href="{{ user.github }}" target="_blank">
                <div class="avatar-wrapper">
                    <img src="{{ user.avatar }}"/>
                </div>
                <div class="title">
                    {% if user.name %}{{user.name}}<br/>{% endif %}
                    @{{user.username}}
                </div>
            </a>
        </div>
    {% endif %}
{% endfor %}
</div>

### RabbitMQ

<div class="user-list user-list-center">
{% for user in people.people %}
    {% if 'rabbitmq' in user.include %}
        <div class="user">
            <a href="{{ user.github }}" target="_blank">
                <div class="avatar-wrapper">
                    <img src="{{ user.avatar }}"/>
                </div>
                <div class="title">
                    {% if user.name %}{{user.name}}<br/>{% endif %}
                    @{{user.username}}
                </div>
            </a>
        </div>
    {% endif %}
{% endfor %}
</div>

### NATS

<div class="user-list user-list-center">
{% for user in people.people %}
    {% if 'nats' in user.include %}
        <div class="user">
            <a href="{{ user.github }}" target="_blank">
                <div class="avatar-wrapper">
                    <img src="{{ user.avatar }}"/>
                </div>
                <div class="title">
                    {% if user.name %}{{user.name}}<br/>{% endif %}
                    @{{user.username}}
                </div>
            </a>
        </div>
    {% endif %}
{% endfor %}
</div>

### Redis

<div class="user-list user-list-center">
{% for user in people.people %}
    {% if 'redis' in user.include %}
        <div class="user">
            <a href="{{ user.github }}" target="_blank">
                <div class="avatar-wrapper">
                    <img src="{{ user.avatar }}"/>
                </div>
                <div class="title">
                    {% if user.name %}{{user.name}}<br/>{% endif %}
                    @{{user.username}}
                </div>
            </a>
        </div>
    {% endif %}
{% endfor %}
</div>

## Become a Contributor

Want to join the list of contributors and become an expert? Start by visiting our [contribution page](./getting-started/contributing/CONTRIBUTING.md){.internal-link} to learn how to set up your local environment for contributing. Once you're ready, head over to our [GitHub issues](https://github.com/ag2ai/faststream/issues){.external-link target="_blank"} page to pick an issue from the existing list, work on it, and submit a [pull request (PR)](https://github.com/ag2ai/faststream/pulls){.external-link target="_blank"}. By contributing, you can become one of the recognized contributors, with your GitHub profile featured on this page. Your expertise and efforts will help further improve the **FastStream** framework and its community!
