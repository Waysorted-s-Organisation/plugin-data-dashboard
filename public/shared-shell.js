(() => {
  const pages = [
    { href: "/", label: "Product Overview", icon: "⌂", paths: ["/"] },
    { href: "/features.html", label: "Feature Intelligence", icon: "◇", paths: ["/features.html"] },
    { href: "/heatmap.html", label: "Heatmap", icon: "⌗", paths: ["/heatmap.html"] },
    { href: "/credits.html", label: "Credits", icon: "◎", paths: ["/credits.html"] },
    { href: "/newsletter-v2.html", label: "Newsletter", icon: "✉", paths: ["/newsletter.html", "/newsletter-v2.html"] },
    { href: "/stats.html", label: "Public Stats", icon: "↗", paths: ["/stats.html"] },
  ];
  const path = window.location.pathname;
  document.body.classList.add("shared-shell-page");
  const sidebar = document.createElement("aside");
  sidebar.className = "shared-sidebar";
  sidebar.id = "sharedSidebar";
  sidebar.innerHTML = `<div class="shared-brand"><span>W</span><strong>Waysorted</strong></div><nav aria-label="Product navigation">${pages.map((page) => {
    const active = page.paths.includes(path);
    return `<a href="${page.href}" class="${active ? "active" : ""}" ${active ? 'aria-current="page"' : ""}><span>${page.icon}</span>${page.label}</a>`;
  }).join("")}</nav><div class="shared-sidebar-note">Owner operations console</div>`;
  document.body.prepend(sidebar);

  const header = document.querySelector(".page-header");
  if (header) {
    const toggle = document.createElement("button");
    toggle.className = "shared-menu-toggle";
    toggle.type = "button";
    toggle.setAttribute("aria-label", "Open navigation");
    toggle.setAttribute("aria-expanded", "false");
    toggle.textContent = "☰";
    toggle.addEventListener("click", () => {
      const open = sidebar.classList.toggle("open");
      toggle.setAttribute("aria-expanded", String(open));
    });
    header.prepend(toggle);
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") sidebar.classList.remove("open");
  });
})();
