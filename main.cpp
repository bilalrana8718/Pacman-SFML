#include <SFML/Graphics.hpp>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>      
#include <time.h>         
#include <cmath>
#include <queue>
#include <vector>
#include "Menu.h"
#include <semaphore.h>
#include <time.h>
#include <list>

using namespace sf;
// using namespace std;


pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;



sem_t menu_sema;

sem_t sema1; //////////between ghost and player  scenario 1

//////// scenario 2 producer consumer ////

sem_t producer_semaphore, consumer_semaphore;
Vector2i powerPelletPosition;

////   scenario 2 producer consumer /////////////

//////////////////scenario 3 dinning philosopher problem /////////////////

#define N 4
#define THINKING 2
#define HUNGRY 1
#define EATING 0
#define LEFT (ghostNum + 3) % N
#define RIGHT (ghostNum) % N

sem_t philosopher_semaphores[N];
sem_t p_mutex1, p_mutex2;
int state[N];


////////////////////////// scenario 4 ///////////////////
//////////////////////// sleeping barbar /////////////////////

sem_t producer4, consumer4;
std::list<int> ghostId;
int count4 = 0;
#define BUFFER_SIZE4 2

///////////////////// scenario 4 ///////////////////////



struct SpeedGhosts{

    float remainingtime = 15;
    int ghostNum1, ghostNum2;
} speed_ghosts;

char map[MAP_HEIGHT][MAP_WIDTH + 10] {
    "#################################################################################################################################################################",
    "#                                                                                                                                                               #",
    "# .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  . #",
    "#                                                                                                                                                               #",
    "#   #########   ########################   ##########################################   #####################    ########   ##################   ############   #",
    "# . #       # . #                      # . ########################################## . ##################### .  #      # . ################## . ############ . #",
    "#   #       #   #                      #   ##########################################   #####################    #      #   ##################   ############   #",    
    "#   #       #   #                      #   ##########################################   #####################    #      #   ##################   ############   #",
    "# . #       # . #                      # . ########################################## . ##################### .  #      # . ################## . ############ . #",
    "#   #########   #                      #   ##########################################   #####################    #      #   ##################   ############   #",
    "#               #                      #                                                                         #      #                                       #",
    "# .  .  .  .  . #                      # .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .                          #      # .  .  .  .  .  .  .  .  .  .  .  .  . #",
    "#               #       ################                                                                         #      #                                       #",
    "#############   #       #                  #####################   ##################   #####################    #      #   #####################   #############",
    "            # . #       # .  .  .  .  .  . ##################### . ################## . #                   # .  #      # . #####               # . #            ",
    "            #   #       #                  #####################   ##################   #                   #    #      #   #####               #   #            ",
    "            #   #       #   ############   #####################   ##################   #                   #    #      #   #####               #   #            ",
    "            # . #       # . #          # . ##################### . ################## . #                   # .  #      # . #####               # . #            ",
    "            #   #       #   #          #   #####################   ##################   #                   #    #      #   #####               #   #            ",
    "            #   #       #   #          #   #####################   ##################   #                   #    #      #   #####               #   #            ",
    "            # . #       # . #          # . ##################### . ################## . ##################### .  #      # . #####               # . #            ",
    "#############   #       #   #######    #   #####################   ##################                  ######    #      #   #####               #   #############",
    "                #       #         #    #   #####################   ##################                  ######    #      #   #####               #                ",
    "  .  .  .  .  . #       # .  .  . #    # . ##################### . ##################                  ######    #      # . #####               # .              ",
    "                #       #         #    #   #####################   ##################                  ######    #      #   #####   #############                ",
    "#############   #       ###########    #   #####################   ##################   ######         ######    #      #   #####   #               #############",
    "            # . #                      # . ##################### . ################## . ###### .  .  . ###### .  #      # . #####   # .  .  .  .  . #            ",
    "            #   #                      #   #####################   ##################   ######         ######    #      #   #####   #               #            ",
    "            #   #                      #   #####################   ##################   #####################    #      #   #####   #############   #            ",
    "            # . #                      # . ##################### . ################## . ##################### .  #      # . #####               # . #            ",
    "            #   #                      #   #####################   ##################   #####################    #      #   #####               #   #            ",
    "            #   #                      #   #####################   ##################                            #      #   #####################   #            ",
    "            # . #                      # . ##################### . ################## .  .  .  .  .  .  .  .  .  #      # . ##################### . #            ",
    "#############   ########################   #####################   ##################                            ########   #####################   #############",
    "#                                                                                       #####################                                                   #",
    "# .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  . ##################### .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  . #",
    "#                                                                                       #####################                                                   #",
    "#   ############   ###########################   ######   ###########################            ###            ###########################   ###############   #",
    "# . #          # . ########################### . #    # . ########################### .  .  .  . ### .  .  .  . ########################### . ############### . #",
    "#   #          #   ###########################   #    #   ###########################            ###            ###########################   ###############   #", 
    "#   #          #   ###########################   #    #   ###########################   ######   ###   ######   ###########################   ###############   #",   
    "# . #          # . ########################### . #    # . ###########################   #    # . ### . #    # . ########################### . ############### . #",
    "#   ############   ###########################   #    #   ###########################   #    #   ###   #    #   ###########################   ###############   #",
    "#                                                #    #                                 #    #         #    #                                                   #",
    "# .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  . #    # .  .  .  .  .  .  .  .  .  .  . #    # .  .  . #    # .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  . #",
    "#                                                #    #                                 #    #         #    #                                                   #",
    "##################################################    ###################################    ###########    #####################################################"
};    

///////////////////////////////// CLass pacman ////////////////////////////////////////////////////////////////////////////
void initialize_ghost();
    bool checkWalls(Vector2i pos){
        for(int i= pos.x-1 ; i <= pos.x+1 ; i++){
            for(int j= pos.y-1 ; j <= pos.y+1 ; j++){
                if(map[j][i] == '#')
                    return true;
            }
        }
        return false;
    }
// Pacman class
class Pacman {
private:
    
    int speed = 1;
    int nextMovement = 0;
public:
    Vector2i position;
    int lives = 4;
    int points = 0;
    float powerRemaining = 0;
    int movementType = 0;//right at the beginning
    Pacman() {
        position.x = 97;
        position.y = 44;
    }

    Vector2i getposition(){
        return position;
    }
    void setPosition(Vector2i p){
        position.x = p.x;
        position.y = p.y;
    }
    void changeDirection(int moveType) {
        nextMovement = moveType;
    }
    void changeDirection(){
        
        if(nextMovement == 3  && !checkWalls({position.x, position.y-speed})){
            movementType = nextMovement;
        }
        else if(this->nextMovement == 1 && !checkWalls({position.x, position.y+1}))           
            movementType = nextMovement;
        else if(this->nextMovement == 2 && !checkWalls({position.x-1, position.y}))
            movementType = nextMovement;
        else if(this->nextMovement == 0  && !checkWalls({position.x+1, position.y}))       
            movementType = nextMovement;    
    
    }
    void move(){

        Vector2i direction = {0, 0};
        if(this->movementType == 3  && !checkWalls({position.x, position.y-speed})){
            direction . y = -speed;
        }
        else if(this->movementType == 1 && !checkWalls({position.x, position.y+1}))           
            direction . y = speed;
        else if(this->movementType == 2 && !checkWalls({position.x-1, position.y}))
            direction . x = -speed;
        else if(this->movementType == 0  && !checkWalls({position.x+1, position.y}))       
            direction . x = speed;    
        

        sem_wait(&sema1);


            //critical section
            map[position.y][position.x] = ' ';
        
            position += direction;

            // Addition code
            if (position.x < 0)
            {
                position.x = 163;
            }
            if (position.x > 163)
            {
                position.x = 0;
            }

            map[position.y][this->position.x] = 'c';    
    
        sem_post(&sema1);
    }
};

///////////////////////////////// CLass pacman ////////////////////////////////////////////////////////////////////////////

////////////////////////////////struct ghost //////////////////////////////////////////////////////

class  GhostStructure
{
    public:
    Vector2i position;
    int speed = 1;
    char previousChar = ' ';
    int nextMovement = 0;
    int ghostNum = 0;
    int movementType = 0;
    bool isInHome = true;
    int powerRemaining = 0;

    void changeDirection(){
        
        if(nextMovement == 3  && !checkWalls({position.x, position.y-speed})){
            movementType = nextMovement;
        }
        else if(this->nextMovement == 1 && !checkWalls({position.x, position.y+1}))           
            movementType = nextMovement;
        else if(this->nextMovement == 2 && !checkWalls({position.x-1, position.y}))
            movementType = nextMovement;
        else if(this->nextMovement == 0  && !checkWalls({position.x+1, position.y}))       
            movementType = nextMovement;    
    
    }
    void move(){
        bool flag = false;

        Vector2i direction = {0, 0};
        while(!flag){
            if(this->movementType == 3){
                if(!checkWalls({position.x, position.y-speed})){
                    direction . y = -speed;
                    flag = true;
                }
                else{
                    this->movementType = nextMovement;
                    this->nextMovement = rand()%4;
                }

            }
            else if(this->movementType == 1){
                if(!checkWalls({position.x, position.y+1})){
                    direction . y = speed;
                    flag = true;
                }
                else{
                    this->movementType = nextMovement;
                    this->nextMovement = rand()%4;                   
                }
            }           
                
            else if(this->movementType == 2){
                if(!checkWalls({position.x-1, position.y})){
                    direction.x = -speed;
                    flag = true;
                }
                else{
                    this->movementType = nextMovement;
                    this->nextMovement = rand()%4;
                }
            }
            else if(this->movementType == 0){
                if(!checkWalls({position.x+1, position.y})){
                    direction . x = speed;
                    flag = true;
                }
                else{
                    this->movementType = nextMovement;
                    this->nextMovement = rand()%4;
                }
            }       

        }

        sem_wait(&sema1);
        //critical section
            if(previousChar == '.' || previousChar == '@')
                map[position.y][position.x] = previousChar;
            else    
                map[position.y][position.x] = ' ';

            position += direction;
            previousChar = map[position.y][position.x];
            map[position.y][this->position.x] = (char) ghostNum+'0';    


        sem_post(&sema1);
    }        
 
 
};

GhostStructure* ghosts = new GhostStructure[4];
////////////////////////////////struct ghost //////////////////////////////////////////////////////

///////////////////////// scenario 2 ///////////////

// bool powerPalletValidation(Vector2i pos){
//     if((pos.x-2)%3 == 0 && (pos.y-2)%3 == 0 &&(map[pos.y][pos.x] == '.' || map[pos.y][pos.x] == ' ')){
//         return true;
//     }
//     return false;
// }
void* producer_thread(void* arg){

    Vector2i possiblePowerPelletPositions[7] = {{50, 35}, {150, 35}, {158, 2}, {158, 44}, {32, 23}, {2, 11},{65, 11}};

    while(1){
        sem_wait(&producer_semaphore);
        int indTemp = rand()%3;
    
        while(powerPelletPosition == possiblePowerPelletPositions[indTemp])
            indTemp = rand()%3;
        
        powerPelletPosition = possiblePowerPelletPositions[indTemp];
        std:: cout << powerPelletPosition.x << " " << powerPelletPosition.y << std:: endl;
        sem_wait(&sema1);
            map[powerPelletPosition.y][powerPelletPosition.x] = '@';
        sem_post(&sema1);
        sem_post(&consumer_semaphore);
    }
}

void* consumer_thread(void* arg){
    Pacman* p = (Pacman*)arg;
    while(1){
        sem_wait(&consumer_semaphore);
        Vector2i pos = powerPelletPosition;
        while(!(p->getposition().x == pos.x && p->getposition().y == pos.y) ||  p->powerRemaining > 0);

        p->powerRemaining = 10;
        std::cout << p->powerRemaining << std::endl;
        //sleep(1);
        sem_post(&producer_semaphore);
    }
}






//////////////////////// scenario 2 //////////////////



//////////////////////////scenario 3 /////////////////////

void* philosopher(void* arg){
    
    int ghostNum = *((int*)arg);
    sem_post(&p_mutex2);
    while(true){
        while(ghosts[ghostNum].isInHome == false);

        state[ghostNum] = HUNGRY;


        sleep(1);
        if (state[ghostNum] == HUNGRY && state[LEFT] != EATING && state[RIGHT] != EATING) {
            sem_wait(&philosopher_semaphores[LEFT]);
            sem_wait(&philosopher_semaphores[RIGHT]);
            sem_wait(&p_mutex1);
            state[ghostNum] = EATING;   

            //change position
            sem_wait(&sema1);

            map[ghosts[ghostNum].position.y][ghosts[ghostNum].position.x] = ghosts[ghostNum].previousChar;

            ghosts[ghostNum].position = {97, 11};
            
            
            map[ghosts[ghostNum].position.y][ghosts[ghostNum].position.x] = (char) ghostNum + '1';

            ghosts[ghostNum].isInHome = false;
            
            sem_post(&sema1);
            
            
            sleep(2);

            sem_post(&p_mutex1);

            sleep(5);

            state[ghostNum] = THINKING;
            sem_post(&philosopher_semaphores[ghostNum]);
            sem_post(&philosopher_semaphores[LEFT]);
        }

    }

}

/////////////////////scenario 3//////////////////////







////////////////////////// scenario 4 ///////////////

void* producer_thread_scenario4(void* arg){
    while(1){
        sem_wait(&producer4);

            int randInd = rand()%4;
                
            while(count4 > 0 && ghostId.front() == randInd)
                randInd = rand()%4;

            ghostId.push_back(randInd);

            sem_wait(&sema1);
                ghosts[randInd].powerRemaining = 20;
                count4++;
            sem_post(&sema1);
            
        sem_post(&consumer4);
    }
}

void* consumer_thread_scenario4(void* arg){
    Pacman* p = (Pacman*)arg;
    while(1){
        sem_wait(&consumer4);

            while(ghosts[ghostId.front()].powerRemaining > 0);

            ghostId.pop_front();
            
            count4--;

        sem_post(&producer4);
    }
}

//////////////////////// scenario 4 //////////////////







//// //////////////////////////// UI Thread //////////////////////////
void centerlize(CircleShape& pellet){
    pellet . setPosition(pellet . getPosition() . x + (TILE_SIZE - 2*(float)pellet . getRadius())/2, pellet . getPosition() . y +(TILE_SIZE - 2*(float)pellet . getRadius())/2);
}

void showMaze(RenderWindow &window, Pacman p){
    sem_wait(&sema1);

    for (int i = 0; i < MAP_HEIGHT; i++) {
        for (int j = 0; j < MAP_WIDTH; j++) {
            if (map[i][j] == '#') {
                CircleShape circle(TILE_SIZE*4/6);
                circle . setPosition(j * TILE_SIZE, i * TILE_SIZE);
                centerlize(circle);
                circle . setFillColor(Color::Cyan);
                window . draw(circle);
            }
            else if (map[i][j] == '@' || (powerPelletPosition.x == j && powerPelletPosition.y == i)) {
                CircleShape circle(TILE_SIZE*3/4);
                circle . setPosition(j * TILE_SIZE, i * TILE_SIZE);
                centerlize(circle);
                circle . setFillColor(Color::White);
                window . draw(circle);
            }
            else if(map[i][j] == '.') {
                CircleShape circle(TILE_SIZE*3/8);
                circle . setPosition(j * TILE_SIZE, i * TILE_SIZE);
                centerlize(circle);
                circle . setFillColor(Color::Yellow);
                window . draw(circle);
            }
            else if(map[i][j] == 'c') {
                   Texture texture;
                    Sprite sprite;
                std:: string imagepath = "img/pacman0.png";
                if(p.movementType == 0)
                    imagepath = "img/pacman0.png";
                else if(p.movementType == 1)
                    imagepath = "img/pacman1.png";
                else if(p.movementType == 2)
                    imagepath = "img/pacman2.png";
                else if(p.movementType == 3)
                    imagepath = "img/pacman3.png";

                texture . loadFromFile(imagepath);
                sprite . setTexture(texture);
                sprite.setScale(3*TILE_SIZE/(float)256.0, 3*TILE_SIZE/(float)256.0);
                sprite . setPosition((j-1)*TILE_SIZE, (i-1)*TILE_SIZE);
                window . draw(sprite);
            }
            else if(map[i][j] >= '1' && map[i][j] <= '4'){
                Texture texture;
                Sprite sprite;
                std:: string imagepath;
                
                if(p.powerRemaining > 0){
                    texture.loadFromFile("img/blueGhost.png");
                    sprite.setTexture(texture);
                    sprite.setScale(3*TILE_SIZE/(float)280.0, 3*TILE_SIZE/(float)280.0);
                }
                else{
                    if(map[i][j] == '1') {
                        imagepath = "img/ghost1.png";
                    }
                    else if(map[i][j] == '2') {
                        imagepath = "img/ghost2.png";
                    }
                    else if(map[i][j] == '3') {
                        imagepath = "img/ghost3.png";
                    }
                    else if(map[i][j] == '4') {
                        imagepath = "img/ghost4.png";
                    }
                    texture . loadFromFile(imagepath);
                    sprite . setTexture(texture);
                    sprite.setScale(3*TILE_SIZE/(float)109.0, 3*TILE_SIZE/(float)117.0);
                }
                sprite . setPosition((j-1)*TILE_SIZE, (i-1)*TILE_SIZE);
                window . draw(sprite);
            }
        }
        sem_post(&sema1);
    }

    // Addition code
    std::string life = "img/life.png";
    Texture lifeTexture[p.lives];
    Sprite lifeSprite[p.lives];
    for(int i=0; i<p.lives; i++){
        lifeTexture[i].loadFromFile(life);
        lifeSprite[i].setTexture(lifeTexture[i]);
        lifeSprite[i].setScale(3*TILE_SIZE/(float)356.0, 3*TILE_SIZE/(float)356.0);
        lifeSprite[i].setPosition((10 + i*6)*TILE_SIZE, (49)*TILE_SIZE);
        window.draw(lifeSprite[i]);
    }

    Font font;
    font.loadFromFile("JerseyM54.ttf");
    Text score;
    score.setFont(font);
    score.setFillColor(sf::Color::Cyan);
    score.setString("Score : " + std::to_string(p.points));
    score.setCharacterSize(30);
    score.setPosition(140*TILE_SIZE, 49*TILE_SIZE);
    window.draw(score);


}



Vector3i checkCollision(Pacman p){
    sem_wait(&sema1);
    for(int i=p.getposition().x-2; i <=p.getposition().x+2 ; i++) {
        for(int j=p.getposition().y-2; j <=p.getposition().y+2 ; j++) {
            if(map[j][i] >= '1' && map[j][i] <= '4')
            {
                int temp = map[j][i];
                //map[p.getposition().y][p.getposition().x] = ' ';
                sem_post(&sema1);
                return { i, j, temp - '0'};
            }    
        }
    }
    sem_post(&sema1);
    return {0, 0, 0};
}

// Addition code
void checkPellets(Pacman &p){
    for(int i=p.getposition().x-2; i <=p.getposition().x+2 ; i++) {
        for(int j=p.getposition().y-2; j <=p.getposition().y+2 ; j++) {
            if(map[j][i] == '.')
                p.points++;
        }
    }
}

/////////////////////////////// UI Thread //////////////////////////
void cleanMapFromGhosts(){
    for(int i=0 ;i< MAP_HEIGHT ; i++){
        for(int j = 0 ; j<MAP_WIDTH+10 ; j++) {
            if((map[i][j] >= '1' && map[i][j] <= '4') || map[i][j] == 'c')
                map[i][j] =' ';
        }
    }
}
void *MainGameEngine (void *arg)
{
    sem_wait(&menu_sema);
    sem_init(&producer_semaphore, 0,1);
    sem_init(&consumer_semaphore, 0,0);
    sem_init(&producer4, 0,2);
    sem_init(&consumer4, 0,0);

    pthread_t consumerThandle, producerThandle, producerThandle4, consumerThandle4;

    srand(time(0));
    Pacman *pacman = (Pacman* ) arg;
    
    pthread_create(&producerThandle, NULL, producer_thread, NULL);
    pthread_create(&consumerThandle, NULL, consumer_thread, (void*) pacman);
    pthread_create(&producerThandle4, NULL, producer_thread_scenario4,NULL);
    pthread_create(&consumerThandle4, NULL, consumer_thread_scenario4, NULL);
    while (true) {
        sleep(milliseconds(50));
        
        pacman->changeDirection();
        pacman->move();
        Vector3i collisionCheck = checkCollision(*pacman);
        if(collisionCheck.z != 0){
            if(pacman->powerRemaining <=0){
                pacman->lives--;
                if(pacman->lives <= 0)
                {
                    // pthread_mutex_unlock(&mutex1);
                    return NULL;
                    pthread_exit(NULL);
                }
                sleep(1);
                pacman->setPosition({97,44});
                cleanMapFromGhosts();
                initialize_ghost();
                sleep(1);
            }
            else{
                sem_wait(&sema1);
                
                map[collisionCheck.y][collisionCheck.x] = ' ';
                map[18][100] = collisionCheck.z + '0';
                
                ghosts[collisionCheck.z-1].position = {100, 18};
                ghosts[collisionCheck.z-1].isInHome = true;
                
                sem_post(&sema1);


                //////add score////////
                pacman->points += 100;
            }
        }
    checkPellets(*pacman);

    }
    pthread_exit(NULL);
}


void *UI_Function(void * arg)
{
    
    Pacman *pacman = (Pacman* ) arg;

    // addition code 
    int choice  = loading_menu();
    switch(choice){
        case 0: return NULL;
                break;
        case 1: break;
        case 2: // load the intructions menu 
        break;
    }

    sem_post(&menu_sema);
    sem_post(&menu_sema);

    RenderWindow window(VideoMode(WINDOW_WIDTH, WINDOW_HEIGHT + 80), "Pacman Game");
   
    Clock clock;
    while (window.isOpen()) {
        
        float time = clock.getElapsedTime().asSeconds();
        clock.restart();
        sleep(milliseconds(50));
        // pthread_mutex_lock(&mutex1);
        Event event;

        while (window.pollEvent(event)) {
            // pthread_mutex_lock(&mutex1);

            if (event . type == Event::Closed)
                window.close();
            // Handle keyboard input
            if (event . type == Event::KeyPressed) {
                if (event . key . code == Keyboard::Up)
                    pacman->changeDirection(3);
                else if (event . key . code == Keyboard::Down)
                    pacman->changeDirection(1);
                else if (event . key . code == Keyboard::Left)
                    pacman->changeDirection(2);
                else if (event . key . code == Keyboard::Right)
                    pacman->changeDirection(0);
                else if (event . key . code == Keyboard::Escape)
                {
                    window.close();
                    return NULL;
                }
            }
            
        } 
        // speed_ghosts.remainingtime -= time1;
        if(pacman->powerRemaining >=0)
            pacman->powerRemaining -= time;
        for(int i=0 ;i<4 ; i++){
            if(ghosts[i].powerRemaining >= 0)
                ghosts[i].powerRemaining -= time;
        }
        window.clear();
        showMaze(window, *pacman);
        window.display();


            if(pacman->lives <= 0)
            {
                sf::Texture backgroundTexture;
                if (!backgroundTexture.loadFromFile("img/Game_Over_logo.png")) {
                    std::cerr << "Error loading background image" << std::endl;
                    return 0;
                }
                sf::Sprite backgroundSprite(backgroundTexture);
                    Font font;
                font.loadFromFile("JerseyM54.ttf");
                Text score;
                score.setFont(font);
                score.setFillColor(sf::Color::White);
                score.setString("Score : " + std::to_string(pacman->points));
                score.setCharacterSize(30);
                score.setPosition(75*TILE_SIZE, 49*TILE_SIZE);


                backgroundSprite.setPosition(350, 80);
                window.clear();
                window.draw(score);
                window.draw(backgroundSprite);
                window.display();
                sleep(5);
                window.close();
        }
        
    }

    pthread_exit(NULL);

}

/////////////////////////////// Ghost Controller Thread //////////////////////////



// addition code 
void *ghosts_controller_thread(void *arg){

   GhostStructure* ghost = (GhostStructure*)arg;
    while(true){
        
        if(ghost->powerRemaining  > 0)
            sleep(milliseconds(50));
        else{

            sleep(milliseconds(500));
        
        }
        ghost->move();
        ghost->changeDirection();
        
    }
}

void initialize_ghost(){
    for(int i=0; i<4; i++){
        ghosts[i].speed = 1;
        ghosts[i].isInHome = true;
        ghosts[i].position.x = 90 + i*4;
        ghosts[i].position.y = 15;

        ghosts[i].ghostNum = i+1;
        ghosts[i].movementType = rand()%4;
        
        map[ghosts[i].position.y][ghosts[i].position.x] = (char)i+'1';
    }
}

void *GhostController_Function(void *arg)
{
    
    sem_wait(&menu_sema);

    initialize_ghost();

    pthread_t threads[N];

    sem_init(&p_mutex1, 0, 1);
    sem_init(&p_mutex2, 0, 0);

    for(int i=0 ; i<N ; i++){  
        sem_init(&philosopher_semaphores[i], 0, 1);
    }

    for (int i = 0; i < N; i++) {
        pthread_create(&threads[i], NULL, philosopher,(void*) &i);
        sem_wait(&p_mutex2);
    }   

    pthread_t ghosts_controller_thread_create[4];
    for(int i=0; i<4; i++){
        pthread_create(&ghosts_controller_thread_create[i], NULL, ghosts_controller_thread, (void*)&ghosts[i]);
    }


    pthread_exit(NULL);
}



// Main function
int main() {
    pthread_t GameEngine;
    pthread_t UI;
    pthread_t GhostController;

    sem_init(&sema1, 0, 1);
    sem_init(&menu_sema, 0, 0);

    sem_init (&producer4, 0 , 1);
    sem_init (&consumer4, 0 , 0);

   
    Pacman *pacman = new Pacman();


    pthread_create(&UI, NULL, UI_Function, (void *) pacman);
    pthread_create(&GameEngine, NULL, MainGameEngine, (void*)pacman);
    pthread_create(&GhostController, NULL, GhostController_Function, (void*)ghosts);
   


    pthread_exit(NULL);

    return 0;
}
