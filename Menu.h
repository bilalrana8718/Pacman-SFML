#include <SFML/Graphics.hpp>
#include <iostream>

// Define constants
const int TILE_SIZE = 10;
const int MAP_WIDTH = 54*3;
const int MAP_HEIGHT = 16*3;
const int WINDOW_WIDTH = TILE_SIZE * MAP_WIDTH;
const int WINDOW_HEIGHT = TILE_SIZE * MAP_HEIGHT;

int loading_menu()
{
    sf::Texture backgroundTexture;
    if (!backgroundTexture.loadFromFile("img/file.png")) {
        std::cerr << "Error loading background image" << std::endl;
        return 0;
    }
    sf::Sprite backgroundSprite(backgroundTexture);

    backgroundSprite.setScale(3*TILE_SIZE/(float)34.0, 3*TILE_SIZE/(float)69.0);
    backgroundSprite.setPosition(430, 50);

    sf::RenderWindow window(sf::VideoMode(WINDOW_WIDTH, WINDOW_HEIGHT), "Pacman Game");
    sf::Font font;
    if (!font.loadFromFile("JerseyM54.ttf")) {
        std::cerr << "Error loading font file" << std::endl;
        return 0;
    }

    sf::Text playtext;
    playtext.setFont(font);
    playtext.setFillColor(sf::Color::White);
    playtext.setString("Play Game");
    playtext.setCharacterSize(30);  
    playtext.setPosition(750.f, 250.f);  

    sf::Text exittext;
    exittext.setFont(font);
    exittext.setFillColor(sf::Color::White);
    exittext.setString("Exit");
    exittext.setCharacterSize(30);  
    exittext.setPosition(750.f, 310.f);   

    while (window.isOpen()) {
        sf::Event event;
        while (window.pollEvent(event)) {
            if (event.type == sf::Event::Closed) {
                window.close();
            }
            else if (event.type == sf::Event::MouseButtonPressed) {
                if (event.mouseButton.button == sf::Mouse::Left) {
                    sf::Vector2f mousePos = window.mapPixelToCoords(sf::Vector2i(event.mouseButton.x, event.mouseButton.y));
                    if (playtext.getGlobalBounds().contains(mousePos)) {
                        // Handle Play Game option
                        return 1;
                    }
                    else if (exittext.getGlobalBounds().contains(mousePos)) {
                        // Handle Exit option
                        return 0;
                    }
                }
            }
        }
        window.clear();
        window.draw(backgroundSprite);
        window.draw(playtext);
        
        window.draw(exittext);
        window.display();
    }

    return 0;
}

