\NeedsTeXFormat{LaTeX2e}

\ProvidesClass{FluigentManual}[2019/05/06 User Manual Template for Fluigent products, V1.4.0]

% Base class
\LoadClass[a4paper, 10pt]{report}

% Options
\RequirePackage{xkeyval}
\RequirePackage{xkvltxp}

% Default values for the options. Currently not working
\newcommand{\manualTitle}{Fluigent Product}
\newcommand{\manualSubtitle}{Manual}
\newcommand{\manualVersion}{1A}
\newcommand{\manualCoverImage}{}
\DeclareOptionX{title}{\renewcommand{\manualTitle}{#1}}
\DeclareOptionX{subtitle}{\renewcommand{\manualSubtitle}{#1}}
\DeclareOptionX{version}{\renewcommand{\manualVersion}{#1}}
\DeclareOptionX{image}{\renewcommand{\manualCoverImage}{#1}}
\ExecuteOptionsX{title, subtitle, version, image}
\ProcessOptionsX

\providecommand{\coverTitle}{\manualTitle}

% Font
\RequirePackage[utf8]{inputenc}
\RequirePackage[T1]{fontenc}
\RequirePackage{helvet}
\RequirePackage{textcomp} % Some special characters such as µ
\renewcommand{\familydefault}{\sfdefault}

% Fluigent colors
\RequirePackage{xcolor}
\definecolor{fgtLightGreen}{HTML}{59B6B2}
\definecolor{fgtLightGreenAlt}{HTML}{41AAA5}
\definecolor{fgtDarkGreen}{HTML}{0A7E95}
\definecolor{fgtDarkGreenAlt}{HTML}{006C86}
\definecolor{fgtOrange}{HTML}{FEA914}
\definecolor{fgtRed}{HTML}{E42B4A}
\definecolor{fgtLightGrey}{HTML}{777777}
\definecolor{fgtGrey}{HTML}{555555}

% Colors for the code listings (based on visual studio)
\definecolor{bluekeywords}{rgb}{0,0,1}
\definecolor{greencomments}{rgb}{0,0.5,0}
\definecolor{redstrings}{rgb}{0.64,0.08,0.08}
\definecolor{xmlcomments}{rgb}{0.5,0.5,0.5}
\definecolor{types}{rgb}{0.17,0.57,0.68}

% Other colors
\definecolor{airforceblue}{rgb}{0.36, 0.54, 0.66}
\definecolor{darkpowderblue}{rgb}{0.0, 0.2, 0.6}

% Chapter header style
\RequirePackage{titlesec}
\newcommand{\hsp}{\hspace{20pt}}
\titleformat{\chapter}[hang]{\Huge\bfseries}{\textcolor{fgtLightGreenAlt}{\thechapter}\hsp\textcolor{fgtLightGreenAlt}{|}\hsp}{0pt}{\Huge\bfseries\textcolor{fgtLightGreenAlt}}


% Page margins
\RequirePackage[
top    = 2cm,
bottom = 2cm,
left   = 2cm,
right  = 2cm]{geometry} 

% Paragraph spacing
\RequirePackage{indentfirst}
\setlength{\parindent}{2em}
\setlength{\parskip}{1em}

% PDF Properties and hyperlinks
\RequirePackage{hyperref}

% Images and tables
\RequirePackage{graphicx}
\RequirePackage{tabularx}

% Table spacing
\newcommand\Tstrut{\rule{0pt}{2.6ex}}         % top strut (add space between rows)
\newcommand\Bstrut{\rule[-0.9ex]{0pt}{0pt}}   % bottom strut (add space after table)
\newcommand{\hlineWithSpace}{\hline\Tstrut}   % horizontal line with extra vertical space

% Code listings
\RequirePackage{listings}
\RequirePackage{{inconsolata}} % Visual Studio like font, replaces the default tt font automatically

% PDF Properties
\hypersetup{
	pdftitle={\manualTitle},
	pdfsubject={\manualTitle\ \manualSubtitle},
	pdfauthor={Fluigent},
	pdfdisplaydoctitle=true,
	bookmarksnumbered=true,     
	bookmarksopen=true,         
	bookmarksopenlevel=1,       
	colorlinks=true,   
	citecolor = black,
	filecolor = black,
	linkcolor = black,
	urlcolor = blue,
	pdfstartview=FitH, 
}

% If statement to test if a string is empty
\RequirePackage{etoolbox}

\newcommand{\monthtwodigits}{
	\ifnum\month>9
	\the\month
	\else
	0\the\month
	\fi
}

\renewcommand{\maketitle}{
	\thispagestyle{empty}
		\begin{center}
			\includegraphics[width=\linewidth]{img/logos/fluigent-logo}
			\\[5cm]
			\huge{\textbf{\coverTitle}}

			\LARGE{\manualSubtitle}
			\vfill
			\ifdefempty{\manualCoverImage}{\vspace{10cm}}{\includegraphics[width=\linewidth, height=10cm, keepaspectratio]{\manualCoverImage}}
			\vfill
		\end{center}
	
	\normalsize{Version \manualVersion\ \monthtwodigits/\the\year}
}

\newcommand{\contactpage}{
	\clearpage
	\thispagestyle{empty}
	\begin{center}
		\includegraphics[width=0.7\linewidth]{img/logos/fluigent-logo}
		\\[5cm]
		\Large
		FLUIGENT 
		
		O’kabé bureaux 
		
		57-77, avenue de Fontainebleau 
		
		94270 Le Kremlin-Bicêtre 
		
		FRANCE 
		
		Phone: +331 77 01 82 68 
		
		Fax: +331 77 01 82 70 
		
		\href{www.fluigent.com}{www.fluigent.com}
		\\[2cm]
		
		Technical support:
		
		\href{mailto:support@fluigent.com?subject=Support request (\manualTitle)}{support@fluigent.com}
		
		Phone : +331 77 01 82 65
		\\[2cm]
		General information:
		
		\href{mailto:contact@fluigent.com?subject=Information request (\manualTitle)}{contact@fluigent.com}
	\end{center}
}

% Base style for code listings

\lstset{
	captionpos=b,
	frame=lines, 
	showspaces=false,
	showtabs=false,
	breaklines=true,
	showstringspaces=false,
	breakatwhitespace=true,
	escapeinside={(*@}{@*)},
	commentstyle=\color{greencomments},	
	keywordstyle=\color{bluekeywords},
	stringstyle=\color{redstrings},
	basicstyle=\ttfamily,
	title=\lstname % show the filename of files included with \lstinputlisting;
}

% Code listing style for C#

\lstdefinestyle{csharp}{ 
	language = [Sharp]C,
	morekeywords={partial, var, value, get, set},
}

% Code listings style for Python

\lstdefinestyle{python}{ 
	language = Python,
}

% Code listings style for C++

\lstdefinestyle{cpp}{ 
	language = C++,
}