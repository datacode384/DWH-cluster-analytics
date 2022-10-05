- As pycharm IDE is not free for ssh development, u can use VS IDE https://code.visualstudio.com/docs/remote/ssh
or
- Make vim more interactive by installing plugins https://howchoo.com/g/ztmyntqzntm/how-to-install-vim-plugins-without-a-plugin-manager
  - https://vimawesome.com/ (nerd tree and gruvbox theme)
  - install them by cloning them in a designated vim directory, then open vim and run the install plugs command
after, you need to create a txt file named .vimrc in the ~ directory
and that will be the config. here is what mine looks like:

```
  1 Specify a directory for plugins
  2 For Neovim: ~/.local/share/nvim/plugged
  3 Avoid using standard Vim directory names like 'plugin'
  4 call plug#begin('~/.vim/plugged')
  6 Plug 'scrooloose/nerdtree'
  7 Plug 'morhetz/gruvbox'
  8 Make sure you use single quotes
 10 Initialize plugin system
 11 call plug#end()
 13 map <C-n> :NERDTreeToggle<CR>
 14 set number
 15 colorscheme gruvbox
 16 set background=dark
```
