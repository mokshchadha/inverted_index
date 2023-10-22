const SEARCH_URL = "http://localhost:3000/search";
const app = {
  searchString: "",
  searchResults: [],
  debounceTimeout: null,

  onSearchInput: function (e) {
    const searchValue = e.target.value;
    app.searchString = searchValue;
    clearTimeout(app.debounceTimeout);
    app.debounceTimeout = setTimeout(() => {
      if (searchValue.length >= 3) {
        m.request(`${SEARCH_URL}?queryText=${searchValue}`).then((e) => {
          console.log(e);
          app.searchResults = e.map((e) => e.review_text);
          console.log(app.searchResults);
        });
      } else {
        app.searchResults = [];
      }
      m.redraw();
    }, 500);
  },

  view: function () {
    return m("div", [
      m("input[type=text]", {
        placeholder: "Search...",
        oninput: app.onSearchInput,
        value: app.searchString,
        // onblur: function (e) {
        //   if (app.searchString.length < 3) {
        //     alert("Search string must be at least 3 characters.");
        //     e.target.focus();
        //   }
        // },
      }),
      m(
        "ul",
        app.searchResults.map((result) => m("li", result))
      ),
    ]);
  },
};

m.mount(document.getElementById("app"), app);
