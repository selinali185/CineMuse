(() => {
  const root = document.querySelector('[data-topbar-search]');
  const toggle = document.querySelector('[data-topbar-search-toggle]');
  const inline = document.querySelector('[data-topbar-search-inline]');
  const input = document.querySelector('[data-topbar-search-input]');
  if (!root || !toggle || !inline || !input) return;

  const open = () => {
    inline.hidden = false;
    input.focus();
  };

  const close = () => {
    inline.hidden = true;
  };

  toggle.addEventListener('click', (event) => {
    event.preventDefault();
    const willOpen = inline.hidden;
    inline.hidden = !willOpen;
    if (willOpen) input.focus();
  });

  document.addEventListener('pointerdown', (event) => {
    if (!root.contains(event.target)) {
      close();
    }
  }, true);

  input.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') close();
    if (event.key === 'Enter') {
      event.preventDefault();
      const q = input.value.trim();
      if (!q) return;
      window.location.assign(`/search?q=${encodeURIComponent(q)}`);
    }
  });
})();

(() => {
  const modal = document.getElementById('favorite-modal');
  if (!modal) return;

  const openButtons = document.querySelectorAll('[data-open-favorite-modal]');
  const closeButton = document.querySelector('[data-close-favorite-modal]');
  const slotInput = document.getElementById('favorite-slot-input');
  const movieIdInput = document.getElementById('favorite-movie-id-input');
  const addButton = document.getElementById('favorite-add-btn');
  const searchInput = document.getElementById('favorite-search');
  const movieButtons = Array.from(document.querySelectorAll('.movie-choice'));

  const clearSelection = () => {
    movieIdInput.value = '';
    addButton.disabled = true;
    movieButtons.forEach((btn) => btn.classList.remove('selected'));
  };

  const openModal = (slot) => {
    slotInput.value = slot;
    searchInput.value = '';
    movieButtons.forEach((btn) => {
      btn.style.display = '';
    });
    clearSelection();
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
    searchInput.focus();
  };

  const closeModal = () => {
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
  };

  openButtons.forEach((btn) => {
    btn.addEventListener('click', (event) => {
      if (event.target.closest('a')) return;
      openModal(btn.dataset.slot);
    });
  });

  if (closeButton) {
    closeButton.addEventListener('click', closeModal);
  }

  modal.addEventListener('click', (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });

  searchInput.addEventListener('input', () => {
    const needle = searchInput.value.trim().toLowerCase();
    movieButtons.forEach((btn) => {
      const title = btn.dataset.title || '';
      btn.style.display = title.includes(needle) ? '' : 'none';
    });
  });

  movieButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      movieButtons.forEach((b) => b.classList.remove('selected'));
      btn.classList.add('selected');
      movieIdInput.value = btn.dataset.movieId;
      addButton.disabled = false;
    });
  });
})();

(() => {
  const root = document.querySelector('[data-popular-week-carousel]');
  const viewport = document.querySelector('[data-popular-week-viewport]');
  const prevBtn = document.querySelector('[data-popular-week-prev]');
  const nextBtn = document.querySelector('[data-popular-week-next]');
  if (!root || !viewport || !prevBtn || !nextBtn) return;

  const getStep = () => viewport.clientWidth;

  prevBtn.addEventListener('click', () => {
    viewport.scrollLeft = Math.max(0, viewport.scrollLeft - getStep());
  });

  nextBtn.addEventListener('click', () => {
    viewport.scrollLeft = viewport.scrollLeft + getStep();
  });
})();

(() => {
  const root = document.querySelector('[data-friends-review-carousel]');
  const viewport = document.querySelector('[data-friends-review-viewport]');
  const prevBtn = document.querySelector('[data-friends-review-prev]');
  const nextBtn = document.querySelector('[data-friends-review-next]');
  if (!root || !viewport || !prevBtn || !nextBtn) return;

  const getStep = () => viewport.clientWidth;

  prevBtn.addEventListener('click', () => {
    viewport.scrollLeft = Math.max(0, viewport.scrollLeft - getStep());
  });

  nextBtn.addEventListener('click', () => {
    viewport.scrollLeft = viewport.scrollLeft + getStep();
  });
})();

(() => {
  const toggle = document.querySelector('[data-profile-menu-toggle]');
  const menu = document.querySelector('[data-profile-menu]');
  if (!toggle || !menu) return;

  toggle.addEventListener('click', (event) => {
    event.stopPropagation();
    menu.classList.toggle('open');
  });

  document.addEventListener('click', (event) => {
    if (!menu.contains(event.target) && !toggle.contains(event.target)) {
      menu.classList.remove('open');
    }
  });
})();

(() => {
  const actionsToggle = document.querySelector('[data-movie-actions-toggle]');
  const actionsMenu = document.querySelector('[data-movie-actions-menu]');
  const openReviewLog = document.querySelector('[data-open-review-log]');
  const openAddPlaylists = document.querySelector('[data-open-add-playlists]');
  const openAddReview = document.querySelector('[data-open-add-review]');
  const reviewModal = document.getElementById('review-log-modal');
  const closeReviewLog = document.querySelector('[data-close-review-log]');
  const addPlaylistsModal = document.getElementById('add-playlists-modal');
  const closeAddPlaylists = document.querySelector('[data-close-add-playlists]');
  const reviewTextInput = document.getElementById('review-text-input');
  const reviewForm = document.querySelector('.review-log-form');
  const editButtons = Array.from(document.querySelectorAll('[data-edit-review]'));
  const editors = Array.from(document.querySelectorAll('[data-review-editor]'));
  const reviewScopes = Array.from(document.querySelectorAll('.review-form-scope'));

  const hasMovieActions = actionsToggle && actionsMenu;
  const hasReviewModal = reviewModal && openReviewLog && closeReviewLog;
  const hasAddPlaylistsModal = addPlaylistsModal && openAddPlaylists && closeAddPlaylists;
  if (!hasMovieActions && !hasReviewModal) return;

  const setupStarRow = (scope) => {
    const ratingInput = scope.querySelector('[data-rating-input]');
    const starButtons = Array.from(scope.querySelectorAll('.star-btn'));
    if (!ratingInput || !starButtons.length) return;

    const renderStars = (ratingValue) => {
      const rating = Number(ratingValue || 0);
      const fullStars = Math.floor(rating);
      const hasHalf = (rating - fullStars) >= 0.5;
      starButtons.forEach((btn) => {
        const star = Number(btn.dataset.starValue || 0);
        let text = '☆';
        let active = false;
        let half = false;

        if (star <= fullStars) {
          text = '★';
          active = true;
        } else if (hasHalf && star === fullStars + 1) {
          text = '★';
          active = false;
          half = true;
        }

        btn.classList.toggle('active', active);
        btn.classList.toggle('half', half);
        btn.textContent = text;
      });
    };

    if (scope.dataset.starsBound !== '1') {
      starButtons.forEach((btn) => {
        btn.addEventListener('click', () => {
          const star = Number(btn.dataset.starValue || 0);
          const current = Number(ratingInput.value || 0);
          let next = star;

          // Click once for whole star; click same star again to cut that star in half.
          if (Math.abs(current - star) < 0.001) {
            next = Math.max(0.5, star - 0.5);
          } else if (Math.abs(current - (star - 0.5)) < 0.001) {
            next = star;
          }

          ratingInput.value = String(next);
          renderStars(next);
        });
      });
      scope.dataset.starsBound = '1';
    }
    renderStars(ratingInput.value);
  };

  const setupLikedToggle = (scope) => {
    const toggle = scope.querySelector('[data-liked-toggle]');
    const likedInput = scope.querySelector('[data-liked-input]');
    if (!toggle || !likedInput) return;

    const renderLiked = (liked) => {
      toggle.classList.toggle('liked', liked);
      toggle.setAttribute('aria-pressed', liked ? 'true' : 'false');
      likedInput.value = liked ? '1' : '0';
    };

    renderLiked((likedInput.value || '0') === '1');
    if (scope.dataset.likeBound !== '1') {
      toggle.addEventListener('click', () => {
        const nextLiked = (likedInput.value || '0') !== '1';
        renderLiked(nextLiked);
      });
      scope.dataset.likeBound = '1';
    }
  };

  reviewScopes.forEach((scope) => {
    setupStarRow(scope);
    setupLikedToggle(scope);
  });

  const closeInlineEditors = () => {
    editors.forEach((editor) => {
      editor.setAttribute('hidden', 'hidden');
    });
  };

  const openInlineEditor = (reviewId) => {
    const target = document.querySelector(`[data-review-editor="${reviewId}"]`);
    if (!target) return;
    closeInlineEditors();
    target.removeAttribute('hidden');
    const textarea = target.querySelector('textarea');
    if (textarea) {
      textarea.focus();
      textarea.setSelectionRange(textarea.value.length, textarea.value.length);
    }
  };

  const openReviewModal = () => {
    reviewModal.classList.add('open');
    reviewModal.setAttribute('aria-hidden', 'false');
  };

  if (hasMovieActions) {
    actionsToggle.addEventListener('click', (event) => {
      event.stopPropagation();
      actionsMenu.classList.toggle('open');
    });
  }

  if (hasReviewModal) {
    openReviewLog.addEventListener('click', () => {
      actionsMenu.classList.remove('open');
      closeInlineEditors();
      openReviewModal();
    });

    if (openAddReview) {
      openAddReview.addEventListener('click', () => {
        closeInlineEditors();
        if (reviewForm) {
          const ratingInput = reviewForm.querySelector('[data-rating-input]');
          const likedInput = reviewForm.querySelector('[data-liked-input]');
          if (ratingInput) {
            ratingInput.value = '';
            setupStarRow(reviewForm);
          }
          if (likedInput) {
            likedInput.value = '0';
            setupLikedToggle(reviewForm);
          }
        }
        if (reviewTextInput) {
          reviewTextInput.value = '';
          reviewTextInput.focus();
        }
      });
    }

    closeReviewLog.addEventListener('click', () => {
      closeInlineEditors();
      reviewModal.classList.remove('open');
      reviewModal.setAttribute('aria-hidden', 'true');
    });

    reviewModal.addEventListener('click', (event) => {
      if (event.target === reviewModal) {
        closeInlineEditors();
        reviewModal.classList.remove('open');
        reviewModal.setAttribute('aria-hidden', 'true');
      }
    });

    editButtons.forEach((btn) => {
      btn.addEventListener('click', () => {
        const targetReviewId = btn.dataset.editReviewTarget;
        if (!targetReviewId) return;
        openReviewModal();
        openInlineEditor(targetReviewId);
      });
    });
  }

  if (hasAddPlaylistsModal) {
    openAddPlaylists.addEventListener('click', () => {
      actionsMenu.classList.remove('open');
      addPlaylistsModal.classList.add('open');
      addPlaylistsModal.setAttribute('aria-hidden', 'false');
    });

    closeAddPlaylists.addEventListener('click', () => {
      addPlaylistsModal.classList.remove('open');
      addPlaylistsModal.setAttribute('aria-hidden', 'true');
    });

    addPlaylistsModal.addEventListener('click', (event) => {
      if (event.target === addPlaylistsModal) {
        addPlaylistsModal.classList.remove('open');
        addPlaylistsModal.setAttribute('aria-hidden', 'true');
      }
    });
  }

  document.addEventListener('click', (event) => {
    if (hasMovieActions && !actionsMenu.contains(event.target) && !actionsToggle.contains(event.target)) {
      actionsMenu.classList.remove('open');
    }
  });
})();

(() => {
  const hasBlockedTag = (value) => {
    if (typeof value !== 'string') return false;
    const v = value.toLowerCase();
    return v.includes('speechify') || v.includes('grammarly');
  };

  const shouldRemove = (node) => {
    if (!(node instanceof Element)) return false;
    return (
      hasBlockedTag(node.id) ||
      hasBlockedTag(node.className) ||
      hasBlockedTag(node.getAttribute('data-testid')) ||
      hasBlockedTag(node.getAttribute('aria-label')) ||
      node.hasAttribute('data-gramm') ||
      node.hasAttribute('data-gramm_editor') ||
      node.hasAttribute('data-gramm_id')
    );
  };

  const removeBlockedNodes = (root = document) => {
    const matches = root.querySelectorAll(
      '[id*="speechify" i], [class*="speechify" i], [data-testid*="speechify" i], [aria-label*="speechify" i], ' +
      '[id*="grammarly" i], [class*="grammarly" i], [data-gramm], [data-gramm_editor], [data-gramm_id], [aria-label*="grammarly" i]'
    );
    matches.forEach((el) => el.remove());
  };

  removeBlockedNodes();

  const observer = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
      mutation.addedNodes.forEach((node) => {
        if (!(node instanceof Element)) return;
        if (shouldRemove(node)) {
          node.remove();
          return;
        }
        removeBlockedNodes(node);
      });
    });
  });

  observer.observe(document.documentElement, { childList: true, subtree: true });
})();

(() => {
  document.addEventListener('submit', (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    const method = (form.getAttribute('method') || 'get').toLowerCase();
    if (method === 'post') {
      sessionStorage.setItem('nav_skip_update', '1');
    }
  }, true);

  const backLink = document.querySelector('[data-back-link]');
  const currentUrl = `${window.location.pathname}${window.location.search}`;
  const isSearchPage = window.location.pathname === '/search';
  const isSearchUrl = (url) => (url || '').startsWith('/search');
  const rawStack = sessionStorage.getItem('nav_stack');
  const stack = rawStack ? JSON.parse(rawStack) : [];
  const cameFromBackArrow = sessionStorage.getItem('nav_back_used') === '1';
  if (cameFromBackArrow) {
    sessionStorage.removeItem('nav_back_used');
  }
  const skipUpdate = sessionStorage.getItem('nav_skip_update') === '1';
  if (skipUpdate) {
    sessionStorage.removeItem('nav_skip_update');
  }

  if (!skipUpdate && !cameFromBackArrow) {
    if (isSearchPage && isSearchUrl(stack[stack.length - 1])) {
      stack[stack.length - 1] = currentUrl;
    } else if (stack[stack.length - 1] !== currentUrl) {
      stack.push(currentUrl);
    }
  }
  sessionStorage.setItem('nav_stack', JSON.stringify(stack));

  if (!backLink) return;
  const fallbackHref = backLink.getAttribute('href') || '/';
  let targetHref = stack.length > 1 ? stack[stack.length - 2] : fallbackHref;
  if (isSearchPage) {
    targetHref = fallbackHref;
    for (let i = stack.length - 2; i >= 0; i -= 1) {
      if (!isSearchUrl(stack[i])) {
        targetHref = stack[i];
        break;
      }
    }
  }
  backLink.setAttribute('href', targetHref);

  backLink.addEventListener('click', (event) => {
    event.preventDefault();
    const updatedRawStack = sessionStorage.getItem('nav_stack');
    const updatedStack = updatedRawStack ? JSON.parse(updatedRawStack) : [];
    if (isSearchPage) {
      while (updatedStack.length && isSearchUrl(updatedStack[updatedStack.length - 1])) {
        updatedStack.pop();
      }
      const prev = updatedStack.length ? updatedStack[updatedStack.length - 1] : fallbackHref;
      sessionStorage.setItem('nav_stack', JSON.stringify(updatedStack));
      sessionStorage.setItem('nav_back_used', '1');
      window.location.assign(prev || fallbackHref);
      return;
    }
    if (updatedStack.length > 1) {
      updatedStack.pop();
      const prev = updatedStack[updatedStack.length - 1];
      sessionStorage.setItem('nav_stack', JSON.stringify(updatedStack));
      sessionStorage.setItem('nav_back_used', '1');
      window.location.assign(prev || fallbackHref);
      return;
    }
    window.location.assign(fallbackHref);
  });
})();

(() => {
  const searchInput = document.getElementById('create-playlist-movie-search');
  const options = Array.from(document.querySelectorAll('.create-playlist-movie-option'));
  if (!searchInput || !options.length) return;

  searchInput.addEventListener('input', () => {
    const needle = searchInput.value.trim().toLowerCase();
    options.forEach((option) => {
      const title = option.dataset.movieTitle || '';
      option.style.display = title.includes(needle) ? '' : 'none';
    });
  });
})();

(() => {
  const searchInput = document.getElementById('playlist-add-movie-search');
  const options = Array.from(document.querySelectorAll('.playlist-add-movie-option'));
  if (!searchInput || !options.length) return;

  searchInput.addEventListener('input', () => {
    const needle = searchInput.value.trim().toLowerCase();
    options.forEach((option) => {
      const title = option.dataset.movieTitle || '';
      option.style.display = title.includes(needle) ? '' : 'none';
    });
  });
})();

(() => {
  const modal = document.getElementById('activity-edit-modal');
  const openButtons = Array.from(document.querySelectorAll('[data-open-activity-edit]'));
  const closeButton = document.querySelector('[data-close-activity-edit]');
  const reviewIdInput = document.getElementById('activity-edit-review-id');
  const movieIdInput = document.getElementById('activity-edit-movie-id');
  const ratingInput = document.getElementById('activity-edit-rating');
  const likedInput = document.getElementById('activity-edit-liked');
  const likedToggle = document.getElementById('activity-edit-liked-toggle');
  const textInput = document.getElementById('activity-edit-text');
  const starButtons = Array.from(modal ? modal.querySelectorAll('.star-btn') : []);
  if (!modal || !openButtons.length || !closeButton || !reviewIdInput || !movieIdInput || !ratingInput || !likedInput || !likedToggle || !textInput) return;

  const renderStars = (ratingValue) => {
    const rating = Number(ratingValue || 0);
    const fullStars = Math.floor(rating);
    const hasHalf = (rating - fullStars) >= 0.5;
    starButtons.forEach((btn) => {
      const star = Number(btn.dataset.starValue || 0);
      let text = '☆';
      let active = false;
      let half = false;
      if (star <= fullStars) {
        text = '★';
        active = true;
      } else if (hasHalf && star === fullStars + 1) {
        text = '★';
        half = true;
      }
      btn.classList.toggle('active', active);
      btn.classList.toggle('half', half);
      btn.textContent = text;
    });
  };

  const renderLiked = (liked) => {
    likedInput.value = liked ? '1' : '0';
    likedToggle.classList.toggle('liked', liked);
    likedToggle.setAttribute('aria-pressed', liked ? 'true' : 'false');
  };

  likedToggle.addEventListener('click', () => {
    renderLiked((likedInput.value || '0') !== '1');
  });

  starButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const star = Number(btn.dataset.starValue || 0);
      const current = Number(ratingInput.value || 0);
      let next = star;
      if (Math.abs(current - star) < 0.001) {
        next = Math.max(0.5, star - 0.5);
      } else if (Math.abs(current - (star - 0.5)) < 0.001) {
        next = star;
      }
      ratingInput.value = String(next);
      renderStars(next);
    });
  });

  const openModal = () => {
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
  };
  const closeModal = () => {
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
  };

  openButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      reviewIdInput.value = btn.dataset.reviewId || '';
      movieIdInput.value = btn.dataset.movieId || '';
      ratingInput.value = btn.dataset.rating || '';
      textInput.value = btn.dataset.text || '';
      renderLiked(btn.dataset.liked === '1');
      renderStars(ratingInput.value);
      openModal();
    });
  });

  closeButton.addEventListener('click', closeModal);
  modal.addEventListener('click', (event) => {
    if (event.target === modal) closeModal();
  });
})();

(() => {
  const overlay = document.getElementById('search-user-panel-overlay');
  const panelContent = document.getElementById('search-user-panel-content');
  const closeBtn = document.querySelector('[data-close-user-panel]');
  if (!overlay || !panelContent || !closeBtn) return;

  const openPanel = (html) => {
    panelContent.innerHTML = html;
    overlay.classList.add('open');
    overlay.setAttribute('aria-hidden', 'false');
  };
  const closePanel = () => {
    overlay.classList.remove('open');
    overlay.setAttribute('aria-hidden', 'true');
  };

  const openFromLink = async (link) => {
    const panelUrl = link.dataset.panelUrl || link.getAttribute('href');
    if (!panelUrl) return;
    try {
      const response = await fetch(panelUrl, { headers: { 'X-Requested-With': 'XMLHttpRequest' } });
      if (!response.ok) {
        window.location.assign(link.href);
        return;
      }
      const html = await response.text();
      openPanel(html);
    } catch (_err) {
      window.location.assign(link.href);
    }
  };

  document.addEventListener('click', (event) => {
    const link = event.target.closest('[data-open-user-panel]');
    if (!link) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    openFromLink(link);
  });

  panelContent.addEventListener('click', async (event) => {
    const btn = event.target.closest('[data-follow-toggle]');
    if (!btn) return;
    event.preventDefault();

    const url = btn.dataset.followUrl;
    if (!url) return;

    btn.disabled = true;
    try {
      const response = await fetch(url, { method: 'POST', headers: { 'X-Requested-With': 'XMLHttpRequest' } });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        btn.disabled = false;
        return;
      }

      btn.classList.toggle('is-following', Boolean(data.is_following));
      btn.textContent = data.is_following
        ? (btn.dataset.followingLabel || 'Unfollow')
        : (btn.dataset.unfollowedLabel || 'Follow');

      const followerCount = panelContent.querySelector('[data-follower-count]');
      if (followerCount && typeof data.follower_count === 'number') {
        followerCount.textContent = String(data.follower_count);
      }
    } catch (_err) {
      // Ignore and keep current state.
    } finally {
      btn.disabled = false;
    }
  });

  closeBtn.addEventListener('click', closePanel);
  overlay.addEventListener('click', (event) => {
    if (event.target === overlay) closePanel();
  });
})();

(() => {
  const btn = document.querySelector('[data-user-profile-follow-toggle]');
  const followerCount = document.querySelector('[data-user-profile-follower-count]');
  if (!btn) return;

  btn.addEventListener('click', async (event) => {
    event.preventDefault();
    const url = btn.dataset.followUrl;
    if (!url) return;

    btn.disabled = true;
    try {
      const response = await fetch(url, { method: 'POST', headers: { 'X-Requested-With': 'XMLHttpRequest' } });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        return;
      }

      btn.classList.toggle('is-following', Boolean(data.is_following));
      btn.textContent = data.is_following
        ? (btn.dataset.followingLabel || 'Unfollow')
        : (btn.dataset.unfollowedLabel || 'Follow');

      if (followerCount && typeof data.follower_count === 'number') {
        followerCount.textContent = String(data.follower_count);
      }
    } catch (_err) {
      // Ignore and keep current state.
    } finally {
      btn.disabled = false;
    }
  });
})();

(() => {
  const modal = document.getElementById('playlist-edit-modal');
  const openBtn = document.querySelector('[data-open-playlist-edit]');
  const closeBtn = document.querySelector('[data-close-playlist-edit]');
  if (!modal || !openBtn || !closeBtn) return;

  openBtn.addEventListener('click', () => {
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
  });

  closeBtn.addEventListener('click', () => {
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
  });

  modal.addEventListener('click', (event) => {
    if (event.target === modal) {
      modal.classList.remove('open');
      modal.setAttribute('aria-hidden', 'true');
    }
  });
})();

(() => {
  const input = document.getElementById('playlist-edit-description');
  const counter = document.getElementById('playlist-description-counter');
  if (!input || !counter) return;

  const max = Number(input.getAttribute('maxlength') || 250);
  const render = () => {
    const count = input.value.length;
    counter.textContent = `${count}/${max} characters`;
  };

  input.addEventListener('input', render);
  render();
})();

(() => {
  const containers = document.querySelectorAll('[data-bio-container]');
  if (!containers.length) return;

  containers.forEach((container) => {
    const button = container.querySelector('.bio-toggle');
    if (!button) return;
    const snippet = container.querySelector('[data-bio-snippet]');
    const full = container.querySelector('[data-bio-full]');

    button.addEventListener('click', () => {
      const currentlyExpanded = button.getAttribute('aria-expanded') === 'true';
      const nextExpanded = !currentlyExpanded;
      button.setAttribute('aria-expanded', String(nextExpanded));
      if (snippet) {
        snippet.hidden = nextExpanded;
      }
      if (full) {
        full.hidden = !nextExpanded;
      }
      const label = button.querySelector('[data-bio-toggle-label]');
      if (label) {
        label.textContent = nextExpanded ? 'show less' : 'show more';
      }
    });
  });
})();

(() => {
  const buttons = document.querySelectorAll('.search-history-remove');
  if (!buttons.length) return;

  buttons.forEach((button) => {
    button.addEventListener('click', async () => {
      const searchId = button.dataset.searchId;
      if (!searchId) return;
      button.disabled = true;
      try {
        const resp = await fetch(`/search/history/${searchId}/delete`, {
          method: 'POST',
          headers: {
            'X-Requested-With': 'XMLHttpRequest',
          },
        });
        if (resp.ok) {
          const item = button.closest('.search-history-item');
          if (item) {
            item.remove();
          }
        }
      } finally {
        button.disabled = false;
      }
    });
  });
})();
