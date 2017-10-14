
# Installation

You just have to copy as _root_ the three _opencga_ files from _completion_ folder into `/etc/bash_completion.d/`:

    cp completion/opencga* /etc/bash_completion.d/
    
Open a new shell terminal and try to use TAB key.


If by any reason these files does not exist you can create them by executing:

    ./bin/utils/opencga-clicompletion.sh [OUTDIR]

where _OUTDIR_ is the optional output folder, if not provided they are copued into bin/utils.