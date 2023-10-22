const SEARCH_URL = "http://localhost:3000/search";

const ListItem = {
  view: (vnodes) => {
    const data = vnodes.attrs;
    console.log({ data });
    const words = data.text
      .split(" ")
      .map((e) => m(SubstringHighlighter, { text: e }));
    return m("li", words);
  },
};

const SubstringHighlighter = {
  view: function (vnode) {
    const text = vnode.attrs.text;
    const substring = app.searchString;

    if (!substring) {
      return m("span", text);
    }

    const parts = text.split(substring);

    return parts.map((part, index) => {
      if (index < parts.length - 1) {
        return [m("strong", substring), m("span", part), m("span", " ")];
      } else {
        return [m("span", part), m("span", " ")];
      }
    });
  },
};

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
        app.searchResults.map((result) => m(ListItem, { text: result }))
      ),
    ]);
  },
};

m.mount(document.getElementById("app"), app);
