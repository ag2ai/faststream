// Material for MkDocs treats a keystroke as a global shortcut (s, f and /
// open search; p and n switch pages) unless focus is in an input. The Context7
// widget renders in a closed shadow root, so from the outside the focused
// element is the widget host, not its input, and every such letter typed into
// the chat opens site search instead.
//
// Stop the event on `document`, before it bubbles to Material's `window`
// listener. Bubble phase only: stopping it during capture would keep the
// keystroke from reaching the widget's input at all.
document.addEventListener("keydown", (event) => {
  if (event.target instanceof Element && event.target.id === "context7-widget") {
    event.stopPropagation();
  }
});
