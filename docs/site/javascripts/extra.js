document.addEventListener("DOMContentLoaded", function () {
  // Populate community section
  const redditUserProfiles = [
    "ryanstephendavis",
    "robot__eyes",
    "phphater7",
    "ElasticFluffyMagnet",
    "grudev",
    "TheGodfatherCC",
    "bachkhois",
    "moosethemucha",
    "__Timtam__",
    "Pale-Cantaloupe3878",
  ];

  async function fetchRedditProfileData() {
    let result = {};
    for (const profile of redditUserProfiles) {
      try {
        const response = await fetch(
          `https://www.reddit.com/user/${profile}/about.json`
        );
        if (response.ok) {
          const data = await response.json();
          result[profile] = data;
        } else {
          console.error(
            `Error fetching data for ${profile}: ${response.status} ${response.statusText}`
          );
        }
      } catch (error) {
        console.error(`Error fetching data for ${profile}: ${error.message}`);
      }
    }

    for (const profileName in result) {
      const profileImgElement = document.querySelector(
        `.testimonialAnchor_iYyG.${profileName} .testimonialProfilePic_wg0d`
      );
      const profileImage =
        result[profileName]["data"]["snoovatar_img"] === ""
          ? result[profileName]["data"]["icon_img"]
          : result[profileName]["data"]["snoovatar_img"];
      profileImgElement.src = profileImage;

      const displayNameElement = document.querySelector(
        `.testimonialAnchor_iYyG.${profileName} .testimonialUserInfo_th5k h6`
      );
      // nosemgrep: javascript.browser.security.insecure-document-method.insecure-document-method
      displayNameElement.innerHTML = result[profileName]["data"]["subreddit"][
        "display_name_prefixed"
      ].replace("u/", "");

      const profileNameElement = document.querySelector(
        `.testimonialAnchor_iYyG.${profileName} .testimonialUserInfo_th5k p`
      );
      // nosemgrep: javascript.browser.security.insecure-document-method.insecure-document-method
      profileNameElement.innerHTML =
        result[profileName]["data"]["subreddit"]["display_name_prefixed"];
    }
  }

  fetchRedditProfileData();

  // Community load more section
  const loadMoreBtn = document.getElementById("community-load-more");
  const containers = document.querySelectorAll(
    "section.features.community .testimonialWrapper_gvoa"
  );
  const containersLen = containers.length;
  let remainingItemLen =
    document.querySelectorAll(
      "section.features.community .testimonialWrapper_gvoa .testimonialAnchor_iYyG"
    ).length - containersLen;

  loadMoreBtn.addEventListener("click", function () {
    containers.forEach((container) => {
      const hiddenItem = container.querySelector(
        ".testimonialAnchor_iYyG.hidden"
      );
      if (hiddenItem) {
        hiddenItem.classList.remove("hidden");
      }
      remainingItemLen--;
      if (remainingItemLen <= containersLen) {
        loadMoreBtn.style.display = "none";
      }
    });
  });
});
